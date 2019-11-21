{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE ViewPatterns #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE TypeApplications #-}

module Parquet.Reader where

import Data.Foldable (traverse_)
import qualified Data.Conduit.List as CL
import qualified Conduit as C
import Control.Monad.Logger
import Parquet.Stream.Reader (readMetadata, Value, readColumnChunk)
import qualified System.IO as IO
import Lens.Micro
import Data.Word (Word32)
import qualified Data.Map as M
import Control.Arrow ((&&&))
import Text.Pretty.Simple (pPrint)

import qualified Parquet.ThriftTypes as TT
import Parquet.Utils (failOnExcept)

readWholeParquetFile :: String -> IO [Record]
readWholeParquetFile inputFp = do
  readMetadata inputFp >>= \case
    Left  err      -> fail $ "Could not read metadata: " <> show err
    Right metadata -> do
      print metadata
      C.runResourceT
        $    C.runConduit
        $    traverse_
               (sourceRowGroup inputFp metadata)
               (metadata ^. TT.pinchField @"row_groups")
        C..| CL.consume

type Cell = (Word32, Word32, Value)
type Record = [Cell]

-- | Streams the values for every column chunk and zips them into records.
--
-- Illustration:
--
-- _____________________
-- | col1 | col2 | col3 |
-- |  1   |   a  |   x  |
-- |  2   |   b  |   y  |
-- |  3   |   c  |   z  |
-- |______|______|______|
--
-- @sourceRowGroup@ yields the following values in a stream:
--
-- (1, a, x)
-- (2, b, y)
-- (3, c, z)
sourceRowGroup
  :: FilePath
  -> TT.FileMetadata
  -> TT.RowGroup
  -> C.ConduitT () Record (C.ResourceT IO) ()
sourceRowGroup fp metadata rg = do
  C.sequenceSources $ map
    (sourceColumnChunk fp metadata)
    (rg ^. TT.pinchField @"column_chunks")

sourceColumnChunk
  :: FilePath
  -> TT.FileMetadata
  -> TT.ColumnChunk
  -> C.ConduitT a (Word32, Word32, Value) (C.ResourceT IO) ()
sourceColumnChunk fp metadata cc = do
  let
    schema_mapping =
      M.fromList
        $  map ((^. TT.pinchField @"name") &&& id)
        $  metadata
        ^. TT.pinchField @"schema"
  let offset = cc ^. TT.pinchField @"file_offset"
  C.transPipe failOnExcept
    $    C.transPipe runStdoutLoggingT
    $    C.bracketP (open_file_and_seek_to offset) IO.hClose C.sourceHandle
    C..| readColumnChunk schema_mapping cc
 where
  open_file_and_seek_to (fromIntegral -> offset) = do
    h <- IO.openBinaryFile fp IO.ReadMode
    IO.hSeek h IO.AbsoluteSeek offset
    pure h
