{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE TypeApplications #-}

module Parquet.Reader where

import Data.Foldable (traverse_)
import qualified Data.Conduit.List as CL
import qualified Data.Conduit.Binary as CB
import Control.Monad.Logger (MonadLogger, runNoLoggingT)
import Control.Monad.Logger.CallStack (logWarn)
import Data.Functor ((<$))
import Parquet.Stream.Reader
  (Value(..), readColumnChunk, ColumnValue(..), decodeConduit)
import Control.Lens
import qualified Data.Map as M
import Control.Arrow ((&&&))
import qualified Data.Text as T
import qualified Data.HashMap.Strict as HM
import Control.Monad.Except
import qualified Data.List.NonEmpty as NE

import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as BS8
import qualified Conduit as C
import System.IO
  (IOMode(ReadMode), openFile, SeekMode(AbsoluteSeek, SeekFromEnd), hSeek)
import Network.HTTP.Types.Status (statusIsSuccessful)
import Network.HTTP.Simple
  (getResponseBody, getResponseStatus, httpSource, parseRequest, Header)
import Network.HTTP.Client (Request(requestHeaders))
import qualified Parquet.ThriftTypes as TT
import Parquet.Utils (failOnExcept)
import qualified Data.Binary.Get as BG
import Parquet.ParquetObject

newtype ParquetSource m = ParquetSource (Integer -> C.ConduitT () BS.ByteString m ())

type Url = String

readMetadata
  :: (MonadError T.Text m, MonadIO m) => ParquetSource m -> m TT.FileMetadata
readMetadata (ParquetSource source) = do
  bs <- C.runConduit (source (-8) C..| CB.take 8)
  case BG.runGetOrFail BG.getWord32le bs of
    Left err -> fail $ "Could not fetch metadata size: " <> show err
    Right (_, _, metadataSize) ->
      fmap (snd . fst)
        $            C.runConduit
        $            source (-(8 + fromIntegral metadataSize))
        C..|         decodeConduit metadataSize
        `C.fuseBoth` pure ()

localParquetFile :: C.MonadResource m => FilePath -> ParquetSource m
localParquetFile fp = ParquetSource $ \pos -> C.sourceIOHandle $ do
  h <- openFile fp ReadMode
  if pos > 0 then hSeek h AbsoluteSeek pos else hSeek h SeekFromEnd pos
  pure h

remoteParquetFile
  :: (C.MonadResource m, C.MonadThrow m, C.MonadIO m) => Url -> ParquetSource m
remoteParquetFile url = ParquetSource $ \pos -> do
  req <- parseRequest url
  let
    rangedReq = req { requestHeaders = mkRangeHeader pos : requestHeaders req }
  httpSource rangedReq call
 where
  mkRangeHeader :: Integer -> Header
  mkRangeHeader pos =
    let rangeVal = if pos > 0 then show pos <> "-" else show pos
    in ("Range", "bytes=" <> BS8.pack rangeVal)

  call req =
    let status = getResponseStatus req
    in
      if statusIsSuccessful status
        then getResponseBody req
        else
          fail
          $  "Non-success response code from remoteParquetFile call: "
          ++ show status

readWholeParquetFile
  :: ( C.MonadThrow m
     , MonadIO m
     , MonadError T.Text m
     , C.MonadResource m
     , MonadLogger m
     )
  => String
  -> m [ParquetObject]
readWholeParquetFile inputFp = do
  metadata <- readMetadata (localParquetFile inputFp)
  C.runConduit
    $    traverse_
           (sourceRowGroup (localParquetFile inputFp) metadata)
           (metadata ^. TT.pinchField @"row_groups")
    C..| CL.consume

type Record = [(ColumnValue, [T.Text])]

sourceParquet :: FilePath -> C.ConduitT () ParquetObject (C.ResourceT IO) ()
sourceParquet fp = runExceptT (readMetadata (localParquetFile fp)) >>= \case
  Left  err      -> fail $ "Could not read metadata: " <> show err
  Right metadata -> C.transPipe runNoLoggingT $ traverse_
    (sourceRowGroup (localParquetFile fp) metadata)
    (metadata ^. TT.pinchField @"row_groups")

sourceRowGroupFromRemoteFile
  :: (C.MonadResource m, C.MonadIO m, C.MonadThrow m, MonadLogger m)
  => String
  -> TT.FileMetadata
  -> TT.RowGroup
  -> C.ConduitT () ParquetObject m ()
sourceRowGroupFromRemoteFile url metadata rg =
  sourceRowGroup (remoteParquetFile url) metadata rg

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
  :: forall m
   . (C.MonadResource m, C.MonadIO m, C.MonadThrow m, MonadLogger m)
  => ParquetSource m
  -> TT.FileMetadata
  -> TT.RowGroup
  -> C.ConduitT () ParquetObject m ()
sourceRowGroup source metadata rg =
  C.sequenceSources
      (map
        (\cc -> sourceColumnChunk source metadata cc
          C..| CL.mapMaybe ((<$> mb_path cc) . (,))
        )
        (rg ^. TT.pinchField @"column_chunks")
      )
    C..| CL.mapMaybeM parse_record
 where
  mb_path :: TT.ColumnChunk -> Maybe [T.Text]
  mb_path cc =
    TT.unField
      .   TT._ColumnMetaData_path_in_schema
      <$> (cc ^. TT.pinchField @"meta_data")

  -- | Given a parquet record (a set of parquet columns, really), converts it to a single JSON-like object.
  --
  -- It does it by traversing the list of columns, converting them to objects and combining them.
  -- Parsing every column yields a ParquetObject and since ParquetObjects are monoids we combine them.
  parse_record :: Record -> m (Maybe ParquetObject)
  parse_record = fmap (fmap mconcat) $ traverse $ \(column, paths) ->
    case NE.nonEmpty paths of
      Nothing -> Nothing <$ logWarn
        (  "parse_record: Record with value "
        <> T.pack (show (_cvValue column))
        <> " does not have any paths. Record data is corrupted."
        )
      Just ne_paths -> parse_column (column, ne_paths)

  -- | Given a parquet column, converts it to a JSON-like object.
  --
  -- For a given column:
  -- { value = "something", definition_level = 3, path = ["field1", "field2", "field3"] }.
  --
  -- We create:
  -- {
  --   "field1": {
  --     "field2": {
  --       "field3": "something
  --     }
  --   }
  -- }
  parse_column :: (ColumnValue, NE.NonEmpty T.Text) -> m (Maybe ParquetObject)
  parse_column (ColumnValue _ 1 _ v, path NE.:| []) =
    pure $ Just $ MkParquetObject $ HM.fromList
      [(path, value_to_parquet_value v)]
  parse_column (ColumnValue _ _ _ _, _ NE.:| []) = do
   -- This case means that we are in the last path element but definition level is not 1.
   -- For example:
   --
  -- { value = "something", definition_level = 5, path = ["field1", "field2", "field3"] }.
   --
   -- And we reached to the following point:
   --
  -- { value = "something", definition_level = 3, path = ["field3"] }.
   --
   -- At this point we should construct the object {"field3": "something"} but this would require
   -- a definition level of 1.
   --
   -- Hence this record is corrupted.
    logWarn
      "parse_column: No more paths exist but we still have definition levels. Column data is corrupted."
    pure Nothing
  parse_column (ColumnValue r d md v, path NE.:| (p : px)) = do
    mb_obj <- parse_column (ColumnValue r (d - 1) md v, p NE.:| px)
    pure $ mb_obj <&> \obj ->
      MkParquetObject $ HM.fromList [(path, ParquetObject obj)]

  value_to_parquet_value :: Value -> ParquetValue
  value_to_parquet_value Null                 = ParquetNull
  value_to_parquet_value (ValueInt64      v ) = ParquetInt v
  value_to_parquet_value (ValueByteString bs) = ParquetString bs

sourceColumnChunk
  :: (C.MonadIO m, C.MonadResource m, C.MonadThrow m, MonadLogger m)
  => ParquetSource m
  -> TT.FileMetadata
  -> TT.ColumnChunk
  -> C.ConduitT () ColumnValue m ()
sourceColumnChunk (ParquetSource source) metadata cc = do
  let
    schema_mapping =
      M.fromList
        $  map ((^. TT.pinchField @"name") &&& id)
        $  metadata
        ^. TT.pinchField @"schema"
  let offset = cc ^. TT.pinchField @"file_offset"
  source (fromIntegral offset)
    C..| C.transPipe failOnExcept (readColumnChunk schema_mapping cc)
