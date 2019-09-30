{-# LANGUAGE BangPatterns      #-}
{-# LANGUAGE FlexibleContexts  #-}
{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes        #-}
{-# LANGUAGE ViewPatterns      #-}

module Parquet.Stream.Reader where

import qualified Conduit                           as C
import           Control.Arrow                     ((&&&))
import           Control.Monad.Except
import           Control.Monad.Logger
import           Data.Bifunctor                    (first)
import qualified Data.Binary.Get                   as BG
import qualified Data.ByteString                   as BS
import qualified Data.ByteString.Lazy              as LBS
import qualified Data.Conduit.Binary               as C
import qualified Data.Conduit.Serialization.Binary as C
import           Data.Foldable                     (traverse_)
import qualified Data.List.NonEmpty                as NE
import qualified Data.Map                          as M
import qualified Data.Text                         as T
import qualified Data.Text.Lazy                    as TL
import           Data.Traversable                  (for)
import           Data.Word                         (Word32, Word64, Word8)
import           Parquet.Decoder                   (decodeBPBE, decodeHybrid)
import           Parquet.Monad
import qualified Parquet.ThriftTypes               as TT
import           Parquet.Utils                     ((<??>))
import qualified Pinch
import           System.IO
import           Text.Pretty.Simple                (pShow)

pLog :: (MonadLogger m, Show a) => a -> m ()
pLog = logInfoN . TL.toStrict . pShow

dataPageReader
  :: PR m
  => M.Map T.Text TT.SchemaElement
  -> TT.DataPageHeader
  -> TT.Type
  -> NE.NonEmpty T.Text
  -> C.ConduitT BS.ByteString BS.ByteString m [Word8]
dataPageReader schema dp_header column_ty ne_path = do
  let Pinch.Field _rep_level_encoding = TT._DataPageHeader_repetition_level_encoding dp_header
  let Pinch.Field num_values         = TT._DataPageHeader_num_values                dp_header
  let Pinch.Field _def_level_encoding = TT._DataPageHeader_definition_level_encoding dp_header
  (max_rep_level, max_def_level) <- calcMaxEncodingLevels schema ne_path
  pLog column_ty
  _rep_data <- readRepetitionLevel ne_path max_rep_level
  _def_data <- readDefinitionLevel schema ne_path max_def_level
  val <- replicateM (fromIntegral num_values) (decodeValue column_ty)
  pLog val
  pure []

data Value =
  ValueInt64 Word64
  | ValueByteString BS.ByteString
  deriving (Show, Eq)

decodeValue
  :: PR m
  => TT.Type
  -> C.ConduitT BS.ByteString BS.ByteString m Value
decodeValue (TT.BYTE_ARRAY _) = do
  !len <- C.sinkGet BG.getWord32le
  ValueByteString . BS.pack <$> replicateM (fromIntegral len) (C.sinkGet BG.getWord8)
decodeValue (TT.INT64 _) =
  ValueInt64 <$> C.sinkGet BG.getWord64le
decodeValue ty =
  throwError $ "Don't know how to decode value of type " <> T.pack (show ty) <> " yet."

readPage
  :: PR m
  => M.Map T.Text TT.SchemaElement
  -> TT.PageHeader
  -> [T.Text]
  -> TT.Type
  -> C.ConduitT BS.ByteString BS.ByteString m [Word8]
readPage schema page_header path column_ty = do
  ne_path <- NE.nonEmpty path <??> "Schema path cannot be empty"
  let Pinch.Field page_size = TT._PageHeader_uncompressed_page_size page_header
  let Pinch.Field mb_dp_header = TT._PageHeader_data_page_header page_header
  buf <- C.sinkGet (BG.getByteString (fromIntegral page_size))
  case mb_dp_header of
    Nothing -> do
      return []
    Just dp_header ->
      C.yield buf C..| dataPageReader schema dp_header column_ty ne_path

readDefinitionLevel
  :: PR m
  => M.Map T.Text TT.SchemaElement
  -> NE.NonEmpty T.Text
  -> Word8
  -> C.ConduitT BS.ByteString BS.ByteString m [Word32]
readDefinitionLevel _ _ 0 = pure []
readDefinitionLevel schema ne_path max_level = do
  last_elem <- M.lookup (NE.head (NE.reverse ne_path)) schema <??> "Schema element could not be found"
  le_rep_ty <- getRepType last_elem
  case le_rep_ty of
    TT.OPTIONAL _ ->
      go
    TT.REPEATED _ ->
      go
    TT.REQUIRED _ ->
      pure []
  where
    go =
      let
        bit_width = floor (logBase 2 (fromIntegral max_level + 1) :: Double)
      in
        C.sinkGet $ decodeHybrid bit_width

-- | TODO(yigitozkavci): Assumption:
-- Rep levels have BP encoding
-- Def levels have
--
-- We have the encoding info already, we can use them to choose the right encoding.
readRepetitionLevel
  :: (C.MonadThrow m, MonadError T.Text m)
  => NE.NonEmpty T.Text -> Word8 -> C.ConduitT BS.ByteString BS.ByteString m [Word32]
readRepetitionLevel ne_path max_level
  | max_level > 0 && NE.length ne_path > 1 =
    C.sinkGet (decodeBPBE max_level)
  | otherwise = pure []

-- | Algorithm:
-- https://blog.twitter.com/engineering/en_us/a/2013/dremel-made-simple-with-parquet.html
calcMaxEncodingLevels :: MonadError T.Text m => M.Map T.Text TT.SchemaElement -> NE.NonEmpty T.Text -> m (Word8, Word8)
calcMaxEncodingLevels schema path = do
  filled_path <- for path $ \name ->
                   M.lookup name schema <??> "Schema Element cannot be found: " <> name
  foldM (\(rep, def) e -> do
      getRepType e >>= \case
        (TT.REQUIRED _) ->
          pure (rep, def)
        (TT.OPTIONAL _) ->
          pure (rep, def + 1)
        (TT.REPEATED _) ->
          pure (rep + 1, def + 1)
    ) (0, 0) filled_path

getRepType :: MonadError T.Text m => TT.SchemaElement -> m TT.FieldRepetitionType
getRepType e =
  let
    Pinch.Field mb_rep_type = TT._SchemaElement_repetition_type e
  in
    mb_rep_type <??> "Repetition type could not be found for elem " <> T.pack (show e)

readContent :: PR m => TT.FileMetadata -> C.ConduitT BS.ByteString BS.ByteString m ()
readContent metadata = do
  !_ <- C.sinkGet BG.getWord32le
  let Pinch.Field schema = TT._FileMetadata_schema metadata
  let schema_mapping = M.fromList $ map (TT.unField . TT._SchemaElement_name &&& id) schema
  let Pinch.Field row_groups = TT._FileMetadata_row_groups metadata
  traverse_ (readRowGroup schema_mapping) row_groups

readRowGroup :: PR m => M.Map T.Text TT.SchemaElement -> TT.RowGroup -> C.ConduitT BS.ByteString BS.ByteString m ()
readRowGroup schema row_group = do
  let Pinch.Field column_chunks = TT._RowGroup_column_chunks row_group
  traverse_ (readColumnChunk schema) column_chunks

validateCompression :: MonadError T.Text m => TT.ColumnMetaData -> m ()
validateCompression metadata =
  let Pinch.Field compression = TT._ColumnMetaData_codec metadata
  in
    case compression of
      TT.UNCOMPRESSED _ ->
        pure ()
      _ ->
        throwError "This library doesn't support compression algorithms yet."

readColumnChunk :: PR m => M.Map T.Text TT.SchemaElement -> TT.ColumnChunk -> C.ConduitT BS.ByteString BS.ByteString m ()
readColumnChunk schema cc = do
  let Pinch.Field mb_metadata = TT._ColumnChunk_meta_data cc
  metadata <- mb_metadata <??> "Metadata could not be found"
  validateCompression metadata

  let Pinch.Field size = TT._ColumnMetaData_total_compressed_size metadata
  let Pinch.Field column_ty = TT._ColumnMetaData_type metadata
  let Pinch.Field path = TT._ColumnMetaData_path_in_schema metadata

  page_header <- decodeConduit size
  -- TODO(yigitozkavci): There are multiple pages in a column chunk.
  -- Read the rest of them. total_compressed_size has the size for all of
  -- the pages, that can be used to see when to end reading.
  !page_content <- readPage schema page_header path column_ty `C.fuseBoth` pure ()
  pLog page_content
  pure ()

failOnError :: Show err => IO (Either err b) -> IO b
failOnError v = v >>= \case
  Left err -> fail $ show err
  Right val -> pure val

readMetadata :: Handle -> IO (Either T.Text TT.FileMetadata)
readMetadata h = do
  liftIO $ hSeek h SeekFromEnd (-8)
  !metadataSize <- liftIO $ runHandleGet h BG.getWord32le
  liftIO $ hSeek h SeekFromEnd (- (8 + fromIntegral metadataSize))

  fmap (fmap fst)
       $ runExceptT
       $ C.runConduit
       $ C.sourceHandle h
    C..| decodeConduit metadataSize
    `C.fuseBoth` pure ()
  -- pure $ fmap fst v

-- | TODO: This is utterly inefficient. Fix it.
decodeSized :: forall a m size. (MonadError T.Text m, MonadIO m, Integral size, Pinch.Pinchable a) => Handle -> size -> m a
decodeSized h size = do
  pos <- liftIO $ hTell h
  !bs <- liftIO $ runHandleGet h $ BG.getByteString (fromIntegral size)
  case first T.pack $ Pinch.decodeWithLeftovers Pinch.compactProtocol $ bs of
    Left err -> throwError err
    Right (leftovers, val) -> do
      let new_pos = fromIntegral $ fromIntegral pos + fromIntegral size - BS.length leftovers
      liftIO (hSeek h AbsoluteSeek new_pos)
      pure val

decodeConduit :: forall a size m. (MonadError T.Text m, MonadIO m, Integral size, Pinch.Pinchable a) => size -> C.ConduitT BS.ByteString BS.ByteString m a
decodeConduit (fromIntegral -> size) = do
  (left, val) <- liftEither . first T.pack . Pinch.decodeWithLeftovers Pinch.compactProtocol . LBS.toStrict =<< C.take size
  C.leftover left
  pure val

-- decode :: forall a. Pinch.Pinchable a => BS.ByteString -> Either T.Text (BS.ByteString, a)
-- decode =

runHandleGet :: Handle -> BG.Get a -> IO a
runHandleGet h get = do
  pos <- hTell h
  !bytes <- LBS.hGetContents h
  case BG.runGetOrFail get bytes of
    Left too_much_info ->
      fail $ "Partial runGetOrFail failed: " <> show too_much_info
    Right (_, consumed, res) -> do
      hSeek h AbsoluteSeek $ fromIntegral (fromIntegral pos + consumed)
      pure res
