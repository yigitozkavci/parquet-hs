{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE TypeApplications #-}

module Parquet.Reader where

import Data.Foldable (traverse_)
import qualified Data.Conduit.List as CL
import qualified Data.Conduit.Binary as CB
import Control.Monad.Logger
import Parquet.Stream.Reader
  (Value(..), readColumnChunk, ColumnValue(..), decodeConduit)
import Lens.Micro
import qualified Data.Map as M
import Control.Arrow ((&&&))
import qualified Data.Text as T
import qualified Data.HashMap.Strict as HM
import Data.Int (Int64)
import Control.Monad.Except

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
import GHC.Generics (Generic)
import Data.Binary (Binary(..))
import Codec.Serialise (Serialise)
import qualified Data.Binary.Get as BG

newtype ParquetSource m = ParquetSource (Integer -> C.ConduitT () BS.ByteString m ())

type Url = String

readMetadata
  :: (MonadError T.Text m, MonadIO m) => ParquetSource m -> m TT.FileMetadata
readMetadata (ParquetSource source) = do
  bs <- C.runConduit (source (-8) C..| CB.take 8)
  liftIO $ print bs
  liftIO $ putStrLn ""
  case BG.runGetOrFail (BG.getWord32le) bs of
    Left err -> fail "Could not fetch metadata size."
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

  -- seekStream :: Int -> C.ConduitT BS.ByteString BS.ByteString m ()
  -- seekStream pos
  --   | pos > 0
  --   = C.drop (fromIntegral pos)
  --   | otherwise
  --   = C.slidingWindowC (-pos)
  --     C..| (C.lastC >>= \case
  --            Nothing ->
  --              fail
  --                $  "Given negative position "
  --                <> show pos
  --                <> " is larger than stream size."
  --            Just l -> C.yieldMany l
  --          )

readWholeParquetFile
  :: (C.MonadThrow m, MonadIO m, MonadError T.Text m, C.MonadResource m)
  => String
  -> m [ParquetValue]
readWholeParquetFile inputFp = do
  metadata <- readMetadata (localParquetFile inputFp)
  C.runConduit
    $    traverse_
           (sourceRowGroup (localParquetFile inputFp) metadata)
           (metadata ^. TT.pinchField @"row_groups")
    C..| CL.consume

type Cell = (ColumnValue, [T.Text])
type Record = [Cell]

sourceParquet :: FilePath -> C.ConduitT () ParquetValue (C.ResourceT IO) ()
sourceParquet fp = runExceptT (readMetadata (localParquetFile fp)) >>= \case
  Left  err      -> fail $ "Could not read metadata: " <> show err
  Right metadata -> do
    traverse_
      (sourceRowGroup (localParquetFile fp) metadata)
      (metadata ^. TT.pinchField @"row_groups")

newtype ParquetObject = MkParquetObject (HM.HashMap T.Text ParquetValue)
  deriving (Eq, Show, Generic, Serialise)

instance Semigroup ParquetObject where
  MkParquetObject hm1 <> MkParquetObject hm2 = MkParquetObject (hm1 <> hm2)

instance Monoid ParquetObject where
  mempty = MkParquetObject mempty

instance Binary ParquetObject where
  put (MkParquetObject hm) = put (HM.toList hm)
  get = MkParquetObject . HM.fromList <$> get

data ParquetValue =
    ParquetObject !ParquetObject
  | ParquetInt !Int64
  | ParquetString !BS.ByteString
  | ParquetNull
  deriving (Eq, Show, Generic, Binary, Serialise)

sourceRowGroupFromRemoteFile
  :: (C.MonadResource m, C.MonadIO m, C.MonadThrow m)
  => String
  -> TT.FileMetadata
  -> TT.RowGroup
  -> C.ConduitT () ParquetValue m ()
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
  :: (C.MonadResource m, C.MonadIO m, C.MonadThrow m)
  => ParquetSource m
  -> TT.FileMetadata
  -> TT.RowGroup
  -> C.ConduitT () ParquetValue m ()
sourceRowGroup source metadata rg =
  C.sequenceSources
      (map
        (\cc -> sourceColumnChunk source metadata cc
          C..| CL.mapMaybe ((<$> mb_path cc) . (,))
        )
        (rg ^. TT.pinchField @"column_chunks")
      )
    C..| CL.mapMaybe parse_record
 where
  mb_path :: TT.ColumnChunk -> Maybe [T.Text]
  mb_path cc =
    TT.unField
      .   TT._ColumnMetaData_path_in_schema
      <$> (cc ^. TT.pinchField @"meta_data")

  parse_record :: Record -> Maybe ParquetValue
  parse_record [] = Just (ParquetObject (MkParquetObject (HM.fromList [])))
  parse_record ((ColumnValue _ 0 _ v, _) : _) = Just $ value_to_json_value v
  parse_record ((ColumnValue{}, []) : _) = Nothing -- Should never happen. TODO(yigitozkavci): Then make this unrepresentable.
  parse_record ((ColumnValue r d md v, path : px) : xs) = do
    obj              <- parse_column (ColumnValue r (d - 1) md v, px)
    ParquetObject hm <- parse_record xs -- TODO: Partial!
    pure $ ParquetObject $ MkParquetObject (HM.fromList [(path, obj)]) <> hm

  parse_column :: (ColumnValue, [T.Text]) -> Maybe ParquetValue
  parse_column (ColumnValue _ 0 _ v , _        ) = Just $ value_to_json_value v
  parse_column (ColumnValue{}       , []       ) = Nothing -- Should never happen
  parse_column (ColumnValue r d md v, path : px) = do
    obj <- parse_column (ColumnValue r (d - 1) md v, px)
    pure $ ParquetObject $ MkParquetObject $ HM.fromList [(path, obj)]


  value_to_json_value :: Value -> ParquetValue
  value_to_json_value Null                 = ParquetNull
  value_to_json_value (ValueInt64      v ) = ParquetInt v
  value_to_json_value (ValueByteString bs) = ParquetString bs

sourceColumnChunk
  :: (C.MonadIO m, C.MonadResource m, C.MonadThrow m)
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
  source (fromIntegral offset) C..| C.transPipe
    (failOnExcept . runStdoutLoggingT)
    (readColumnChunk schema_mapping cc)
 -- where
  -- open_file_and_seek_to (fromIntegral -> offset) = do
  --   h <- IO.openBinaryFile fp IO.ReadMode
  --   IO.hSeek h IO.AbsoluteSeek offset
  --   pure h
