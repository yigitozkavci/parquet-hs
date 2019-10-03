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
import           Control.Monad.Reader
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
import           Parquet.Decoder                   (decodeBPBE,
                                                    decodeRLEBPHybrid)
import           Parquet.Monad
import qualified Parquet.ThriftTypes               as TT
import           Parquet.Utils                     ((<??>))
import qualified Pinch
import           System.IO
import           Text.Pretty.Simple                (pShow)

pLog :: (MonadLogger m, Show a) => a -> m ()
pLog = logInfoN . TL.toStrict . pShow

dataPageReader
  :: (PR m, MonadReader PageCtx m)
  => TT.DataPageHeader
  -> C.ConduitT BS.ByteString BS.ByteString m [Word8]
dataPageReader header = do
  let Pinch.Field rep_level_encoding = TT._DataPageHeader_repetition_level_encoding header
  let Pinch.Field num_values         = TT._DataPageHeader_num_values                header
  let Pinch.Field def_level_encoding = TT._DataPageHeader_definition_level_encoding header
  (max_rep_level, max_def_level) <- calcMaxEncodingLevels
  pLog =<< asks _pcColumnTy
  pLog =<< asks _pcPath
  pLog (max_rep_level, max_def_level)
  pLog =<< asks _pcSchema
  pLog "Reading rep data"
  pLog =<< readRepetitionLevel rep_level_encoding max_rep_level
  pLog "Reading def data"
  pLog =<< readDefinitionLevel def_level_encoding max_def_level
  pLog "Reading value"
  pLog num_values
  val <- replicateM (fromIntegral num_values) decodeValue
  pLog val
  pure []

data Value =
  ValueInt64 Word64
  | ValueByteString BS.ByteString
  deriving (Show, Eq)

decodeValue
  :: (PR m, MonadReader PageCtx m)
  => C.ConduitT BS.ByteString BS.ByteString m Value
decodeValue =
  asks _pcColumnTy >>= \case
    (TT.BYTE_ARRAY _) -> do
      !len <- C.sinkGet BG.getWord32le
      ValueByteString . BS.pack <$> replicateM (fromIntegral len) (C.sinkGet BG.getWord8)
    (TT.INT64 _) ->
      ValueInt64 <$> C.sinkGet BG.getWord64le
    ty ->
      throwError $ "Don't know how to decode value of type " <> T.pack (show ty) <> " yet."

indexPageReader :: TT.IndexPageHeader -> C.ConduitT BS.ByteString BS.ByteString m [Word8]
indexPageReader = undefined

dataPageV2Header :: TT.DataPageHeaderV2 -> C.ConduitT BS.ByteString BS.ByteString m [Word8]
dataPageV2Header = undefined

dictPageReader :: (PR m, MonadReader PageCtx m) => TT.DictionaryPageHeader -> C.ConduitT BS.ByteString BS.ByteString m [Word8]
dictPageReader header = do
  pLog header
  let Pinch.Field num_values = TT._DictionaryPageHeader_num_values header
  let Pinch.Field encoding = TT._DictionaryPageHeader_encoding header
  let Pinch.Field is_sorted = TT._DictionaryPageHeader_is_sorted header
  pLog =<< decodeValue
  pure []

newtype PageReader = PageReader (forall m0. (PR m0, MonadReader PageCtx m0) => C.ConduitT BS.ByteString BS.ByteString m0 [Word8])

mkPageReader
  :: forall m. (MonadError T.Text m, MonadLogger m)
  => TT.PageHeader
  -> m PageReader
mkPageReader page_header = do
  let Pinch.Field mb_data_page_header = TT._PageHeader_data_page_header page_header
  let Pinch.Field mb_index_page_header = TT._PageHeader_index_page_header page_header
  let Pinch.Field mb_dict_page_header = TT._PageHeader_dictionary_page_header page_header
  let Pinch.Field mb_data_page_v2_header = TT._PageHeader_data_page_header_v2 page_header
  pLog (mb_data_page_header, mb_index_page_header, mb_dict_page_header, mb_data_page_v2_header)
  case (mb_data_page_header, mb_index_page_header, mb_dict_page_header, mb_data_page_v2_header) of
    (Just dp_header, _, _, _) ->
      pure $ PageReader $ dataPageReader dp_header
    (_, Just ix_header, _, _) ->
      pure $ PageReader $ indexPageReader ix_header
    (_, _, Just dict_header, _) ->
      pure $ PageReader $ dictPageReader dict_header
    (_, _, _, Just dp_v2_header) ->
      pure $ PageReader $ dataPageV2Header dp_v2_header
    _ ->
      throwError "Page header doesn't exist."

data PageCtx = PageCtx
  { _pcSchema   :: M.Map T.Text TT.SchemaElement
  , _pcPath     :: NE.NonEmpty T.Text
  , _pcColumnTy :: TT.Type
  } deriving (Show, Eq)

readPage
  :: (MonadReader PageCtx m, PR m)
  => TT.PageHeader
  -> C.ConduitT BS.ByteString BS.ByteString m [Word8]
readPage page_header = do
  let Pinch.Field page_size = TT._PageHeader_uncompressed_page_size page_header

  PageReader pageReader <- mkPageReader page_header
  buf <- C.sinkGet (BG.getByteString (fromIntegral page_size))
  C.yield buf C..| pageReader

getLastSchemaElement :: (MonadError T.Text m, MonadReader PageCtx m) => m TT.SchemaElement
getLastSchemaElement = do
  path <- asks _pcPath
  schema <- asks _pcSchema
  M.lookup (NE.head (NE.reverse path)) schema <??> "Schema element could not be found"

readDefinitionLevel
  :: (PR m, MonadReader PageCtx m)
  => TT.Encoding
  -> Word8
  -> C.ConduitT BS.ByteString BS.ByteString m [Word32]
readDefinitionLevel _ 0 = pure []
readDefinitionLevel encoding max_level = do
  getLastSchemaElement >>= getRepType >>= \case
    TT.OPTIONAL _ ->
      decodeLevel encoding max_level
    TT.REPEATED _ ->
      decodeLevel encoding max_level
    TT.REQUIRED _ ->
      pure []

decodeLevel
  :: (C.MonadThrow m, MonadError T.Text m, MonadLogger m)
  => TT.Encoding
  -> Word8
  -> C.ConduitT BS.ByteString BS.ByteString m [Word32]
decodeLevel encoding max_level = do
  let bit_width = floor (logBase 2 (fromIntegral max_level) :: Double) + 1
  pLog max_level
  pLog "Bit width"
  pLog bit_width
  case encoding of
    TT.RLE _ ->
      C.sinkGet $ decodeRLEBPHybrid bit_width
    TT.BIT_PACKED _ ->
      C.sinkGet $ decodeBPBE bit_width
    _ ->
      throwError "Only RLE and BIT_PACKED encodings are supported for definition levels"

readRepetitionLevel
  :: (C.MonadThrow m, MonadError T.Text m, MonadReader PageCtx m, MonadLogger m)
  => TT.Encoding
  -> Word8
  -> C.ConduitT BS.ByteString BS.ByteString m [Word32]
readRepetitionLevel encoding max_level = do
  path <- asks _pcPath
  if max_level > 0 && NE.length path > 1 then
    decodeLevel encoding max_level
  else
    pure []

-- | Algorithm:
-- https://blog.twitter.com/engineering/en_us/a/2013/dremel-made-simple-with-parquet.html
calcMaxEncodingLevels :: (MonadReader PageCtx m, MonadError T.Text m) => m (Word8, Word8)
calcMaxEncodingLevels = do
  schema <- asks _pcSchema
  path <- asks _pcPath
  filled_path <- for path $ \name ->
                   M.lookup name schema <??> "Schema Element cannot be found: " <> name
  foldM (\(rep, def) e ->
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
  ne_path <- NE.nonEmpty path <??> "Schema path cannot be empty"
  let page_ctx = PageCtx schema ne_path column_ty
  !page_content <- C.runReaderC page_ctx $ readPage page_header `C.fuseBoth` pure ()
  pLog page_content
  pure ()

failOnError :: Show err => IO (Either err b) -> IO b
failOnError v = v >>= \case
  Left err -> fail $ show err
  Right val -> pure val

readMetadata :: Handle -> IO (Either T.Text TT.FileMetadata)
readMetadata h = do
  liftIO $ hSeek h SeekFromEnd (-8)
  !metadataSize <- liftIO $ run_handle_get BG.getWord32le
  liftIO $ hSeek h SeekFromEnd (- (8 + fromIntegral metadataSize))
  fmap (fmap fst)
       $ runExceptT
       $ C.runConduit
       $ C.sourceHandle h
    C..| decodeConduit metadataSize
    `C.fuseBoth` pure ()
  where
    run_handle_get :: BG.Get a -> IO a
    run_handle_get get = do
      pos <- hTell h
      !bytes <- LBS.hGetContents h
      case BG.runGetOrFail get bytes of
        Left too_much_info ->
          fail $ "Partial runGetOrFail failed: " <> show too_much_info
        Right (_, consumed, res) -> do
          hSeek h AbsoluteSeek $ fromIntegral (fromIntegral pos + consumed)
          pure res

decodeConduit :: forall a size m. (MonadError T.Text m, MonadIO m, Integral size, Pinch.Pinchable a) => size -> C.ConduitT BS.ByteString BS.ByteString m a
decodeConduit (fromIntegral -> size) = do
  (left, val) <- liftEither . first T.pack . Pinch.decodeWithLeftovers Pinch.compactProtocol . LBS.toStrict =<< C.take size
  C.leftover left
  pure val
