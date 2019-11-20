{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE ViewPatterns #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE TypeApplications #-}

module Main where

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

main :: IO ()
main = do
  let fp = "test.parquet"
  IO.openFile fp IO.ReadMode >>= readMetadata >>= \case
    Left  err      -> fail $ "Could not read metadata: " <> show err
    Right metadata -> do
      let
        chunks =
          [ cc
          | rg <- metadata ^. TT.pinchField @"row_groups"
          , cc <- rg ^. TT.pinchField @"column_chunks"
          ]
      result <- C.runResourceT $ C.runConduit $ traverse
        (\c -> sourceColumnChunk fp metadata c C..| CL.consume)
        chunks
      pPrint result

sourceColumnChunk
  :: FilePath
  -> TT.FileMetadata
  -> TT.ColumnChunk
  -> C.ConduitT a [(Word32, Word32, Value)] (C.ResourceT IO) ()
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
