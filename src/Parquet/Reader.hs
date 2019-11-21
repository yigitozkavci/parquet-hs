{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE ViewPatterns #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE TypeApplications #-}

module Parquet.Reader where

import Data.Foldable (traverse_)
import qualified Data.Conduit.List as CL
import qualified Conduit as C
import Control.Monad.Logger
import Parquet.Stream.Reader
  (readMetadata, Value(..), readColumnChunk, ColumnValue(..))
import qualified System.IO as IO
import Lens.Micro
import Data.Word (Word32)
import qualified Data.Map as M
import Control.Arrow ((&&&))
import Text.Pretty.Simple (pPrint)
import qualified Data.Aeson as JSON
import qualified Data.Text as T
import qualified Data.HashMap.Strict as HM
import qualified Data.Text.Encoding as T

import qualified Parquet.ThriftTypes as TT
import Parquet.Utils (failOnExcept)

readWholeParquetFile :: String -> IO [JSON.Value]
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

type Cell = (ColumnValue, [T.Text])
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
  -> C.ConduitT () JSON.Value (C.ResourceT IO) ()
sourceRowGroup fp metadata rg = do
  C.sequenceSources
      (map
        (\cc -> sourceColumnChunk fp metadata cc
          C..| CL.mapMaybe ((<$> mb_path cc) . (,))
        )
        (rg ^. TT.pinchField @"column_chunks")
      )
    C..| CL.mapMaybe parse_record
 where
  mb_path :: TT.ColumnChunk -> Maybe [T.Text]
  mb_path cc =
    (   TT.unField
    .   TT._ColumnMetaData_path_in_schema
    <$> (cc ^. TT.pinchField @"meta_data")
    )

  parse_record :: Record -> Maybe JSON.Value
  parse_record [] = Just (JSON.Object (HM.fromList []))
  parse_record ((ColumnValue r 0 md v, px) : _) = do
    value_to_json_value v
  parse_record ((ColumnValue r d md v, []) : _) = do
    Nothing -- Should never happen
  parse_record ((ColumnValue r d md v, (path : px)) : xs) = do
    obj            <- parse_column (ColumnValue r (d - 1) md v, px)
    JSON.Object hm <- parse_record xs -- TODO: Partial!
    pure $ JSON.Object $ HM.fromList [(path, obj)] <> hm

  parse_column :: (ColumnValue, [T.Text]) -> Maybe JSON.Value
  parse_column (ColumnValue r 0 md v, px) = do
    value_to_json_value v
  parse_column (ColumnValue r d md v, []) = do
    Nothing -- Should never happen
  parse_column (ColumnValue r d md v, (path : px)) = do
    obj <- parse_column (ColumnValue r (d - 1) md v, px)
    pure $ JSON.Object $ HM.fromList [(path, obj)]


  value_to_json_value :: Value -> Maybe JSON.Value
  value_to_json_value Null                 = Just JSON.Null
  value_to_json_value (ValueInt64      v ) = Just $ JSON.Number $ fromIntegral v
  value_to_json_value (ValueByteString bs) = case T.decodeUtf8' bs of
    Left  e -> Nothing
    Right t -> Just (JSON.String t)

sourceColumnChunk
  :: FilePath
  -> TT.FileMetadata
  -> TT.ColumnChunk
  -> C.ConduitT a ColumnValue (C.ResourceT IO) ()
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
