{-# LANGUAGE BangPatterns        #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RankNTypes          #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications    #-}
{-# LANGUAGE ViewPatterns        #-}

module Parquet.Stream.Reader where

import qualified Conduit                           as C
import           Control.Applicative               (liftA3)
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
import           Data.Functor                      (($>))
import           Data.Int                          (Int64)
import qualified Data.List.NonEmpty                as NE
import qualified Data.Map                          as M
import qualified Data.Text                         as T
import qualified Data.Text.Lazy                    as TL
import           Data.Traversable                  (for)
import           Data.Word                         (Word32, Word8)
import           Lens.Micro
import           Parquet.Decoder                   (BitWidth (..), decodeBPBE,
                                                    decodeRLEBPHybrid, decodeVarint)
import           Parquet.Monad
import qualified Parquet.ThriftTypes               as TT
import           Parquet.Utils                     ((<??>))
import qualified Pinch
import           System.IO
import           Text.Pretty.Simple                (pShow)

pLog :: forall a m. (MonadLogger m, Show a) => a -> m ()
pLog = logInfoN . TL.toStrict . pShow

pLogS :: (MonadLogger m) => T.Text -> m ()
pLogS = pLog

-- | TODO: This is so unoptimized that my eyes bleed.
replicateMSized :: (Monad m) => Int -> m (Int64, result) -> m (Int64, [result])
replicateMSized 0 _ = pure (0, [])
replicateMSized n comp = do
  (consumed, result) <- comp
  (rest_consumed, rest_result) <- replicateMSized (n - 1) comp
  pure (consumed + rest_consumed, rest_result ++ [result])

maxLevelToBitWidth 0 = BitWidth 0
maxLevelToBitWidth max_level =
  BitWidth $ floor (logBase 2 (fromIntegral max_level) :: Double) + 1

dataPageReader
  :: forall m. (PR m, MonadReader PageCtx m)
  => TT.DataPageHeader
  -> C.ConduitT BS.ByteString BS.ByteString m (Int64, [Value])
dataPageReader header = do
  pLogS "Reading a data page"
  pLog header

  let num_values = header ^. TT.pinchField @"num_values"
  let def_level_encoding = header ^. TT.pinchField @"definition_level_encoding"
  let rep_level_encoding = header ^. TT.pinchField @"repetition_level_encoding"
  (max_rep_level, max_def_level) <- calcMaxEncodingLevels
  pLogS "Reading rep data"
  (rep_consumed, rep_data) <- readRepetitionLevel rep_level_encoding (maxLevelToBitWidth max_rep_level)
  pLog (rep_consumed, rep_data)
  pLogS "Reading def data"
  (def_consumed, def_data) <- readDefinitionLevel def_level_encoding (maxLevelToBitWidth max_def_level)
  pLog (def_consumed, def_data)
  pLogS "Reading values"

  (val_consumed, vals) <- case header ^. TT.pinchField @"encoding" of
        TT.PLAIN _ -> do
          let non_null_num_values = if null def_data then fromIntegral num_values else length (filter (== fromIntegral max_def_level) def_data)
          (val_consumed, vals) <- replicateMSized (fromIntegral non_null_num_values) decodeValue
          pLogS "Data Page Values:"
          pLog vals
          pure (val_consumed, vals)
        TT.PLAIN_DICTIONARY _ -> do
          !bit_width <- C.sinkGet BG.getWord8
          (val_consumed, vals) <- C.sinkGet $ sizedGet $ decodeRLEBPHybrid $ BitWidth bit_width
          -- We add 1 for getWord8 above.
          pure (val_consumed + 1, [])
        other ->
          throwError $ "Don't know how to encode data pages with encoding: " <> T.pack (show other)

  pure (rep_consumed + def_consumed + val_consumed, vals)

data Value =
  ValueInt64 Int64
  | ValueByteString BS.ByteString
  deriving (Show, Eq)

decodeValue
  :: (PR m, MonadReader PageCtx m)
  => C.ConduitT BS.ByteString BS.ByteString m (Int64, Value)
decodeValue =
  asks _pcColumnTy >>= \case
    (TT.BYTE_ARRAY _) -> do
      !len <- C.sinkGet BG.getWord32le
      pLog $ "Decoding byte array with len " <> show len
      (consumed, result) <- replicateMSized (fromIntegral len) (C.sinkGet (sizedGet BG.getWord8))
      -- We add 4 for getWord32le above.
      pure (consumed + 4, ValueByteString (BS.pack (reverse result)))
    (TT.INT64 _) -> do
      pLogS "Decoding Int64"
      (consumed, result) <- C.sinkGet (sizedGet BG.getInt64le)
      pLog result
      pure (consumed, ValueInt64 result)
    ty ->
      throwError $ "Don't know how to decode value of type " <> T.pack (show ty) <> " yet."

indexPageReader :: TT.IndexPageHeader -> C.ConduitT BS.ByteString BS.ByteString m (Int64, [Value])
indexPageReader = undefined

dataPageV2Header :: TT.DataPageHeaderV2 -> C.ConduitT BS.ByteString BS.ByteString m (Int64, [Value])
dataPageV2Header = undefined

dictPageReader :: (PR m, MonadReader PageCtx m) => TT.DictionaryPageHeader -> C.ConduitT BS.ByteString BS.ByteString m (Int64, [Value])
dictPageReader header = do
  pLogS "Reading a dictionary page"
  pLog header
  let num_values = header ^. TT.pinchField @"num_values"
  let _encoding = header ^. TT.pinchField @"encoding"
  let _is_sorted = header ^. TT.pinchField @"is_sorted"
  (consumed, vals) <- replicateMSized (fromIntegral num_values) decodeValue
  pure (consumed, vals)

newtype PageReader = PageReader (forall m. (PR m, MonadReader PageCtx m) => C.ConduitT BS.ByteString BS.ByteString m (Int64, [Value]))

mkPageReader
  :: forall m. (MonadError T.Text m, MonadLogger m)
  => TT.PageHeader
  -> m PageReader
mkPageReader page_header = do
  pLog page_header
  let mb_data_page_header = page_header ^. TT.pinchPos @5
  let mb_index_page_header = page_header ^. TT.pinchPos @6
  let mb_dict_page_header = page_header ^. TT.pinchPos @7
  let mb_data_page_v2_header = page_header ^. TT.pinchPos @8
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
  -> C.ConduitT BS.ByteString BS.ByteString m (Int64, [Value])
readPage page_header = do
  PageReader pageReader <- mkPageReader page_header
  pageReader

getLastSchemaElement :: (MonadError T.Text m, MonadReader PageCtx m) => m TT.SchemaElement
getLastSchemaElement = do
  path <- asks _pcPath
  schema <- asks _pcSchema
  M.lookup (NE.head (NE.reverse path)) schema <??> "Schema element could not be found"

readDefinitionLevel
  :: (PR m, MonadReader PageCtx m)
  => TT.Encoding
  -> BitWidth
  -> C.ConduitT BS.ByteString BS.ByteString m (Int64, [Word32])
readDefinitionLevel _ (BitWidth 0) = pure (0, [])
readDefinitionLevel encoding bit_width = do
  getLastSchemaElement >>= getRepType >>= \case
    TT.OPTIONAL _ ->
      decodeLevel encoding bit_width
    TT.REPEATED _ ->
      decodeLevel encoding bit_width
    TT.REQUIRED _ ->
      pure (0, [])

sizedGet g = do
  (before, result, after) <- liftA3 (,,) BG.bytesRead g BG.bytesRead
  pure (after - before, result)

decodeLevel
  :: (C.MonadThrow m, MonadError T.Text m, MonadLogger m)
  => TT.Encoding
  -> BitWidth
  -> C.ConduitT BS.ByteString BS.ByteString m (Int64, [Word32])
decodeLevel _ (BitWidth 0) = pure (0, [])
decodeLevel encoding bit_width = do
  case encoding of
    TT.RLE _ ->
      C.sinkGet $ sizedGet $ BG.getWord32le *> decodeRLEBPHybrid bit_width
    TT.BIT_PACKED _ ->
      C.sinkGet $ sizedGet $ BG.getWord32le *> decodeBPBE bit_width
    _ ->
      throwError "Only RLE and BIT_PACKED encodings are supported for levels"

readRepetitionLevel
  :: (C.MonadThrow m, MonadError T.Text m, MonadReader PageCtx m, MonadLogger m)
  => TT.Encoding
  -> BitWidth
  -> C.ConduitT BS.ByteString BS.ByteString m (Int64, [Word32])
readRepetitionLevel encoding bit_width = do
  path <- asks _pcPath
  if NE.length path > 1 then
    decodeLevel encoding bit_width
  else
    pure (0, [])

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
  e ^. TT.pinchField @"repetition_type" <??> "Repetition type could not be found for elem " <> T.pack (show e)

readContent :: PR m => TT.FileMetadata -> C.ConduitT BS.ByteString BS.ByteString m ()
readContent metadata = do
  !_ <- C.sinkGet BG.getWord32le
  let schema = metadata ^. TT.pinchField @"schema"
  let schema_mapping = M.fromList $ map (TT.unField . TT._SchemaElement_name &&& id) schema
  let row_groups = metadata ^. TT.pinchField @"row_groups"
  traverse_ (readRowGroup schema_mapping) row_groups

readRowGroup :: PR m => M.Map T.Text TT.SchemaElement -> TT.RowGroup -> C.ConduitT BS.ByteString BS.ByteString m ()
readRowGroup schema row_group = do
  let column_chunks = row_group ^. TT.pinchField @"column_chunks"
  traverse_ (readColumnChunk schema) column_chunks

validateCompression :: MonadError T.Text m => TT.ColumnMetaData -> m ()
validateCompression metadata =
  let compression = metadata ^. TT.pinchField @"codec"
  in
    case compression of
      TT.UNCOMPRESSED _ ->
        pure ()
      _ ->
        throwError "This library doesn't support compression algorithms yet."

readColumnChunk :: PR m => M.Map T.Text TT.SchemaElement -> TT.ColumnChunk -> C.ConduitT BS.ByteString BS.ByteString m ()
readColumnChunk schema cc = do
  let mb_metadata = cc ^. TT.pinchField @"meta_data"
  metadata <- mb_metadata <??> "Metadata could not be found"
  validateCompression metadata

  pLog metadata
  let size = metadata ^. TT.pinchField @"total_compressed_size"
  let column_ty = metadata ^. TT.pinchField @"type"
  let path = metadata ^. TT.pinchField @"path_in_schema"

  -- TODO(yigitozkavci): There are multiple pages in a column chunk.
  -- Read the rest of them. total_compressed_size has the size for all of
  -- the pages, that can be used to see when to end reading.
  ne_path <- NE.nonEmpty path <??> "Schema path cannot be empty"
  let page_ctx = PageCtx schema ne_path column_ty
  page_content <- C.runReaderC page_ctx $ read_page size

  pLogS "Size:"
  pLog size
  pLogS "Page content:"
  pLog page_content
  pure ()
  where
    read_page 0 = pure []
    read_page remaining = do
      -- | TODO:
      -- C++ client implementation does the following:
      -- It starts by allowing 16KiB of size and doubles it everytime parsing fails.
      -- It allows page sizes up to 16MiB.
      --
      -- We can also do that in this library. For now, let's stick with the lazy solution.
      (page_header_size, page_header) <- decodeConduit remaining
      ((page_consumed, page_result), ()) <- readPage page_header `C.fuseBoth` pure ()
      let page_content_size = page_header ^. TT.pinchField @"uncompressed_page_size"
      let page_size = fromIntegral page_header_size + page_content_size
      pLog $ "Page Header Size: " <> T.pack (show page_header_size)
      pLog $ "Page Content Size: " <> T.pack (show page_content_size)
      pLog $ "Consumed: " <> T.pack (show page_consumed)
      unless (fromIntegral page_content_size == page_consumed) $
        throwError "Reader did not consume the whole page!"
      pLog $ "Remaining: " <> T.pack (show $ remaining - fromIntegral page_size)
      (++ page_result) <$> read_page (remaining - fromIntegral page_size)

failOnError :: Show err => IO (Either err b) -> IO b
failOnError v = v >>= \case
  Left err -> fail $ show err
  Right val -> pure val

readMetadata :: Handle -> IO (Either T.Text TT.FileMetadata)
readMetadata h = do
  liftIO $ hSeek h SeekFromEnd (-8)
  !metadataSize <- liftIO $ run_handle_get BG.getWord32le
  liftIO $ hSeek h SeekFromEnd (- (8 + fromIntegral metadataSize))
  fmap (fmap (snd . fst))
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

decodeConduit :: forall a size m. (MonadError T.Text m, MonadIO m, Integral size, Pinch.Pinchable a) => size -> C.ConduitT BS.ByteString BS.ByteString m (Int, a)
decodeConduit (fromIntegral -> size) = do
  (left, val) <- liftEither . first T.pack . Pinch.decodeWithLeftovers Pinch.compactProtocol . LBS.toStrict =<< C.take size
  C.leftover left
  pure (size - BS.length left, val)
