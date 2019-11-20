{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE MultiWayIf #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE ViewPatterns #-}

module Parquet.Stream.Reader where

import qualified Conduit as C
import qualified Data.Conduit.Combinators as C
import qualified Data.Conduit.List as CL
import Control.Applicative (liftA3)
import Control.Arrow ((&&&), (***), second)
import Control.Monad.Except
import Control.Monad.Logger
import Control.Monad.Reader
import Data.Bifunctor (first)
import qualified Data.Binary.Get as BG
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as LBS
import qualified Data.Conduit.Binary as CB
import qualified Data.Conduit.Serialization.Binary as CB
import Data.Foldable (traverse_)
import Data.Int (Int32, Int64)
import qualified Data.List.NonEmpty as NE
import qualified Data.Map as M
import qualified Data.Text as T
import qualified Data.Text.Lazy as TL
import Data.Traversable (for)
import Data.Word (Word32, Word8)
import Lens.Micro
import qualified Pinch
import System.IO
import Text.Pretty.Simple (pShow)
import Safe.Exact (zipExactMay)

import Parquet.Decoder (BitWidth(..), decodeBPBE, decodeRLEBPHybrid)
import Parquet.Monad
import qualified Parquet.ThriftTypes as TT
import Parquet.Utils ((<??>))

pLog :: forall  a m . (MonadLogger m, Show a) => a -> m ()
pLog = logInfoN . TL.toStrict . pShow

pLogS :: (MonadLogger m) => T.Text -> m ()
pLogS = pLog

-- | TODO: This is so unoptimized that my eyes bleed.
replicateMSized :: (Monad m) => Int -> m (Int64, result) -> m (Int64, [result])
replicateMSized 0 _    = pure (0, [])
replicateMSized n comp = do
  (consumed     , result     ) <- comp
  (rest_consumed, rest_result) <- replicateMSized (n - 1) comp
  pure (consumed + rest_consumed, result : rest_result)

forSized :: (Monad m) => [a] -> (a -> m (Int64, result)) -> m (Int64, [result])
forSized = flip traverseSized

traverseSized
  :: (Monad m) => (a -> m (Int64, result)) -> [a] -> m (Int64, [result])
traverseSized _    []       = pure (0, [])
traverseSized comp (x : xs) = do
  (consumed     , result     ) <- comp x
  (rest_consumed, rest_result) <- traverseSized comp xs
  pure (consumed + rest_consumed, result : rest_result)

maxLevelToBitWidth :: Word8 -> BitWidth
maxLevelToBitWidth 0 = BitWidth 0
maxLevelToBitWidth max_level =
  BitWidth $ floor (logBase 2 (fromIntegral max_level) :: Double) + 1

dataPageReaderS
  :: forall m
   . (PR m, MonadReader PageCtx m)
  => TT.DataPageHeader
  -> Maybe [Value]
  -> C.ConduitT BS.ByteString [(Word32, Word32, Value)] m ()
dataPageReaderS header mb_dict = do
  let num_values         = header ^. TT.pinchField @"num_values"
  let def_level_encoding = header ^. TT.pinchField @"definition_level_encoding"
  let rep_level_encoding = header ^. TT.pinchField @"repetition_level_encoding"
  let encoding           = header ^. TT.pinchField @"encoding"
  (max_rep_level, max_def_level) <- calcMaxEncodingLevels
  (rep_consumed , fill_level_default num_values -> rep_data) <-
    readRepetitionLevel
      rep_level_encoding
      (maxLevelToBitWidth max_rep_level)
      num_values
  (def_consumed, fill_level_default num_values -> def_data) <-
    readDefinitionLevel
      def_level_encoding
      (maxLevelToBitWidth max_def_level)
      num_values
  level_data <- zip_level_data rep_data def_data
  read_page_content encoding level_data num_values (fromIntegral max_def_level)
  pure ()
 where
  find_from_dict
    :: forall  m0 . MonadError T.Text m0 => [Value] -> Word32 -> m0 Value
  find_from_dict dict (fromIntegral -> index) = case dict ^? ix index of
    Nothing ->
      throwError $ "A dictionary value couldn't be found in index " <> T.pack
        (show index)
    Just val -> pure val

  zip_level_data
    :: forall  m0 a b . MonadError T.Text m0 => [a] -> [b] -> m0 [(a, b)]
  zip_level_data rep_data def_data =
    zipExactMay rep_data def_data
      <??> (  "Repetition and Definition data sizes differ: page has "
           <> T.pack (show (length rep_data))
           <> " repetition values and "
           <> T.pack (show (length def_data))
           <> " definition values."
           )

  fill_level_default :: Int32 -> [Word32] -> [Word32]
  fill_level_default num_values = \case
    [] -> replicate (fromIntegral num_values) 1
    xs -> xs

  read_page_content
    :: TT.Encoding
    -> [(a, Word32)]
    -> Int32
    -> Word32
    -> C.ConduitT BS.ByteString [(a, Word32, Value)] m ()
  read_page_content encoding level_data num_values max_def_level =
    case (mb_dict, encoding) of
      (Nothing, TT.PLAIN _) -> do
        (_val_consumed, vals) <- forSized level_data $ \(r, d) -> if
          | d == max_def_level -> fmap (r, d, ) <$> decodeValue
          | otherwise          -> pure (0, (r, d, Null))
        C.yield vals
      (Just _, TT.PLAIN _) -> throwError
        "We shouldn't have PLAIN-encoded data pages with a dictionary."
      (Just dict, TT.PLAIN_DICTIONARY _) -> do
        !bit_width  <- CB.sinkGet BG.getWord8
        val_indexes <- CB.sinkGet
          $ decodeRLEBPHybrid (BitWidth bit_width) num_values
        vals <- construct_dict_values max_def_level dict level_data val_indexes
        C.yield vals
      (Nothing, TT.PLAIN_DICTIONARY _) ->
        throwError
          "Data page has PLAIN_DICTIONARY encoding but we don't have a dictionary yet."
      other ->
        throwError
          $  "Don't know how to encode data pages with encoding: "
          <> T.pack (show other)

  -- | Given repetition and definition level data, a dictionary and a set of indexes,
  -- constructs values for this dictionary-encoded page.
  construct_dict_values
    :: forall m0 rep def
     . (MonadError T.Text m0, Eq def)
    => def
    -> [Value]
    -> [(rep, def)]
    -> [Word32]
    -> m0 [(rep, def, Value)]
  construct_dict_values _ _ [] _ = pure []
  construct_dict_values _ _ _ [] =
    throwError
      "There are not enough level data for given amount of dictionary indexes."
  construct_dict_values max_def_level dict ((r, d) : lx) (v : vx)
    | d == max_def_level = do
      val <- find_from_dict dict v
      ((r, d, val) :) <$> construct_dict_values max_def_level dict lx vx
    | otherwise = do
      ((r, d, Null) :) <$> construct_dict_values max_def_level dict lx (v : vx)

dataPageReader
  :: forall m
   . (PR m, MonadReader PageCtx m)
  => TT.DataPageHeader
  -> Maybe [Value]
  -> C.ConduitT
       BS.ByteString
       BS.ByteString
       m
       (Int64, [(Word32, Word32, Value)])
dataPageReader header mb_dict = do
  let num_values         = header ^. TT.pinchField @"num_values"
  let def_level_encoding = header ^. TT.pinchField @"definition_level_encoding"
  let rep_level_encoding = header ^. TT.pinchField @"repetition_level_encoding"
  let encoding           = header ^. TT.pinchField @"encoding"
  (max_rep_level, max_def_level) <- calcMaxEncodingLevels
  (rep_consumed , fill_level_default num_values -> rep_data) <-
    readRepetitionLevel
      rep_level_encoding
      (maxLevelToBitWidth max_rep_level)
      num_values
  (def_consumed, fill_level_default num_values -> def_data) <-
    readDefinitionLevel
      def_level_encoding
      (maxLevelToBitWidth max_def_level)
      num_values
  level_data               <- zip_level_data rep_data def_data
  (content_consumed, vals) <- read_page_content
    encoding
    level_data
    num_values
    (fromIntegral max_def_level)
  pure (rep_consumed + def_consumed + content_consumed, vals)
 where
  find_from_dict
    :: forall  m0 . MonadError T.Text m0 => [Value] -> Word32 -> m0 Value
  find_from_dict dict (fromIntegral -> index) = case dict ^? ix index of
    Nothing ->
      throwError $ "A dictionary value couldn't be found in index " <> T.pack
        (show index)
    Just val -> pure val

  zip_level_data
    :: forall  m0 a b . MonadError T.Text m0 => [a] -> [b] -> m0 [(a, b)]
  zip_level_data rep_data def_data =
    zipExactMay rep_data def_data
      <??> (  "Repetition and Definition data sizes differ: page has "
           <> T.pack (show (length rep_data))
           <> " repetition values and "
           <> T.pack (show (length def_data))
           <> " definition values."
           )

  fill_level_default :: Int32 -> [Word32] -> [Word32]
  fill_level_default num_values = \case
    [] -> replicate (fromIntegral num_values) 1
    xs -> xs

  read_page_content
    :: TT.Encoding
    -> [(a, Word32)]
    -> Int32
    -> Word32
    -> C.ConduitT
         BS.ByteString
         BS.ByteString
         m
         (Int64, [(a, Word32, Value)])
  read_page_content encoding level_data num_values max_def_level =
    case (mb_dict, encoding) of
      (Nothing, TT.PLAIN _) -> do
        (val_consumed, vals) <- forSized level_data $ \(r, d) -> if
          | d == max_def_level -> fmap (r, d, ) <$> decodeValue
          | otherwise          -> pure (0, (r, d, Null))

        pure (val_consumed, vals)
      (Just _, TT.PLAIN _) -> throwError
        "We shouldn't have PLAIN-encoded data pages with a dictionary."
      (Just dict, TT.PLAIN_DICTIONARY _) -> do
        !bit_width                  <- CB.sinkGet BG.getWord8
        (val_consumed, val_indexes) <- CB.sinkGet $ sizedGet $ decodeRLEBPHybrid
          (BitWidth bit_width)
          num_values
        -- We add 1 for getWord8 above.
        vals <- construct_dict_values max_def_level dict level_data val_indexes
        pure (val_consumed + 1, vals)
      (Nothing, TT.PLAIN_DICTIONARY _) ->
        throwError
          "Data page has PLAIN_DICTIONARY encoding but we don't have a dictionary yet."
      other ->
        throwError
          $  "Don't know how to encode data pages with encoding: "
          <> T.pack (show other)

  -- | Given repetition and definition level data, a dictionary and a set of indexes,
  -- constructs values for this dictionary-encoded page.
  construct_dict_values
    :: forall m0 rep def
     . (MonadError T.Text m0, Eq def)
    => def
    -> [Value]
    -> [(rep, def)]
    -> [Word32]
    -> m0 [(rep, def, Value)]
  construct_dict_values _ _ [] _ = pure []
  construct_dict_values _ _ _ [] =
    throwError
      "There are not enough level data for given amount of dictionary indexes."
  construct_dict_values max_def_level dict ((r, d) : lx) (v : vx)
    | d == max_def_level = do
      val <- find_from_dict dict v
      ((r, d, val) :) <$> construct_dict_values max_def_level dict lx vx
    | otherwise = do
      ((r, d, Null) :) <$> construct_dict_values max_def_level dict lx (v : vx)

data Value
  = ValueInt64 Int64
  | ValueByteString BS.ByteString
  | Null
  deriving (Show, Eq)

decodeValue
  :: (PR m, MonadReader PageCtx m)
  => C.ConduitT BS.ByteString o m (Int64, Value)
decodeValue = asks _pcColumnTy >>= \case
  (TT.BYTE_ARRAY _) -> do
    !len               <- CB.sinkGet BG.getWord32le
    (consumed, result) <- replicateMSized
      (fromIntegral len)
      (CB.sinkGet (sizedGet BG.getWord8))
    pure (consumed + 4, ValueByteString (BS.pack result))
  (TT.INT64 _) -> do
    (consumed, result) <- CB.sinkGet (sizedGet BG.getInt64le)
    pure (consumed, ValueInt64 result)
  ty ->
    throwError
      $  "Don't know how to decode value of type "
      <> T.pack (show ty)
      <> " yet."

dictPageReader
  :: (MonadReader PageCtx m, PR m)
  => TT.DictionaryPageHeader
  -> C.ConduitT BS.ByteString o m (Int64, [Value])
dictPageReader header = do
  let num_values = header ^. TT.pinchField @"num_values"
  let _encoding  = header ^. TT.pinchField @"encoding"
  let _is_sorted = header ^. TT.pinchField @"is_sorted"
  (consumed, vals) <- replicateMSized (fromIntegral num_values) decodeValue
  pure (consumed, vals)

data PageCtx =
  PageCtx
    { _pcSchema :: M.Map T.Text TT.SchemaElement
    , _pcPath :: NE.NonEmpty T.Text
    , _pcColumnTy :: TT.Type
    }
  deriving (Show, Eq)

getLastSchemaElement
  :: (MonadError T.Text m, MonadReader PageCtx m) => m TT.SchemaElement
getLastSchemaElement = do
  path   <- asks _pcPath
  schema <- asks _pcSchema
  M.lookup (NE.head (NE.reverse path)) schema
    <??> "Schema element could not be found"

readDefinitionLevel
  :: (PR m, MonadReader PageCtx m)
  => TT.Encoding
  -> BitWidth
  -> Int32
  -> C.ConduitT BS.ByteString a m (Int64, [Word32])
readDefinitionLevel _ (BitWidth 0) _ = pure (0, [])
readDefinitionLevel encoding bit_width num_values =
  getLastSchemaElement >>= getRepType >>= \case
    TT.OPTIONAL _ -> decodeLevel encoding bit_width num_values
    TT.REPEATED _ -> decodeLevel encoding bit_width num_values
    TT.REQUIRED _ -> pure (0, [])

readRepetitionLevel
  :: (C.MonadThrow m, MonadError T.Text m, MonadReader PageCtx m, MonadLogger m)
  => TT.Encoding
  -> BitWidth
  -> Int32
  -> C.ConduitT BS.ByteString a m (Int64, [Word32])
readRepetitionLevel encoding bit_width num_values = do
  path <- asks _pcPath
  if NE.length path > 1
    then decodeLevel encoding bit_width num_values
    else pure (0, [])

sizedGet :: BG.Get result -> BG.Get (Int64, result)
sizedGet g = do
  (before, result, after) <- liftA3 (,,) BG.bytesRead g BG.bytesRead
  pure (after - before, result)

decodeLevel
  :: (C.MonadThrow m, MonadError T.Text m, MonadLogger m, MonadReader PageCtx m)
  => TT.Encoding
  -> BitWidth
  -> Int32
  -> C.ConduitT BS.ByteString a m (Int64, [Word32])
decodeLevel _        (BitWidth 0) _                            = pure (0, [])
decodeLevel encoding bit_width (fromIntegral -> num_values) = case encoding of
  TT.RLE _ ->
    CB.sinkGet
      $  sizedGet
      $  BG.getWord32le
      *> decodeRLEBPHybrid bit_width num_values
  TT.BIT_PACKED _ ->
    CB.sinkGet
      $  sizedGet
      $  BG.getWord32le
      *> (take (fromIntegral num_values) <$> decodeBPBE bit_width)
  _ -> throwError "Only RLE and BIT_PACKED encodings are supported for levels"

-- | Algorithm:
-- https://blog.twitter.com/engineering/en_us/a/2013/dremel-made-simple-with-parquet.html
calcMaxEncodingLevels
  :: (MonadReader PageCtx m, MonadError T.Text m) => m (Word8, Word8)
calcMaxEncodingLevels = do
  schema      <- asks _pcSchema
  path        <- asks _pcPath
  filled_path <- for path $ \name ->
    M.lookup name schema <??> "Schema Element cannot be found: " <> name
  foldM
    (\(rep, def) e -> getRepType e >>= \case
      (TT.REQUIRED _) -> pure (rep, def)
      (TT.OPTIONAL _) -> pure (rep, def + 1)
      (TT.REPEATED _) -> pure (rep + 1, def + 1)
    )
    (0, 0)
    filled_path

getRepType
  :: MonadError T.Text m => TT.SchemaElement -> m TT.FieldRepetitionType
getRepType e =
  e
    ^.   TT.pinchField @"repetition_type"
    <??> "Repetition type could not be found for elem "
    <>   T.pack (show e)

readContent
  :: PR m => TT.FileMetadata -> C.ConduitT BS.ByteString BS.ByteString m ()
readContent metadata = do
  !_ <- CB.sinkGet BG.getWord32le
  let schema = metadata ^. TT.pinchField @"schema"
  let
    schema_mapping =
      M.fromList $ map (TT.unField . TT._SchemaElement_name &&& id) schema
  let row_groups = metadata ^. TT.pinchField @"row_groups"
  traverse_ (readRowGroup schema_mapping) row_groups

readRowGroup
  :: PR m
  => M.Map T.Text TT.SchemaElement
  -> TT.RowGroup
  -> C.ConduitT BS.ByteString BS.ByteString m ()
readRowGroup schema row_group = do
  pLogS "Reading a row group"
  let column_chunks = row_group ^. TT.pinchField @"column_chunks"
  traverse_ (readColumnChunk schema) column_chunks

validateCompression :: MonadError T.Text m => TT.ColumnMetaData -> m ()
validateCompression metadata =
  let compression = metadata ^. TT.pinchField @"codec"
  in
    case compression of
      TT.UNCOMPRESSED _ -> pure ()
      _ ->
        throwError "This library doesn't support compression algorithms yet."

readColumnChunk
  :: PR m
  => M.Map T.Text TT.SchemaElement
  -> TT.ColumnChunk
  -> C.ConduitT BS.ByteString BS.ByteString m ()
readColumnChunk schema cc = do
  pLogS "Reading a column chunk"
  let mb_metadata = cc ^. TT.pinchField @"meta_data"
  metadata <- mb_metadata <??> "Metadata could not be found"
  validateCompression metadata
  let size      = metadata ^. TT.pinchField @"total_compressed_size"
  let column_ty = metadata ^. TT.pinchField @"type"
  let path      = metadata ^. TT.pinchField @"path_in_schema"
  ne_path <- NE.nonEmpty path <??> "Schema path cannot be empty"
  let page_ctx = PageCtx schema ne_path column_ty

  -- | TODO:
  -- C++ client implementation does the following:
  -- It starts by allowing 16KiB of size and doubles it everytime parsing fails.
  -- It allows page sizes up to 16MiB.
  --
  -- We can also do that in this library. For now, let's stick with the lazy solution.
  page_content <- C.runReaderC page_ctx $ readPage size Nothing
  -- pLogS "Page content:"
  -- pLog page_content
  pure ()

readColumnChunkS
  :: PR m
  => M.Map T.Text TT.SchemaElement
  -> TT.ColumnChunk
  -> C.ConduitT BS.ByteString [(Word32, Word32, Value)] m ()
readColumnChunkS schema cc = do
  let mb_metadata = cc ^. TT.pinchField @"meta_data"
  metadata <- mb_metadata <??> "Metadata could not be found"
  -- validateCompression metadata
  let size      = metadata ^. TT.pinchField @"total_compressed_size"
  let column_ty = metadata ^. TT.pinchField @"type"
  let path      = metadata ^. TT.pinchField @"path_in_schema"
  ne_path <- NE.nonEmpty path <??> "Schema path cannot be empty"
  let page_ctx = PageCtx schema ne_path column_ty

  -- | TODO:
  -- C++ client implementation does the following:
  -- It starts by allowing 16KiB of size and doubles it everytime parsing fails.
  -- It allows page sizes up to 16MiB.
  --
  -- We can also do that in this library. For now, let's stick with the lazy solution.
  C.runReaderC page_ctx $ readPageS size Nothing

readPage
  :: (MonadReader PageCtx m, PR m)
  => Int64
  -> Maybe [Value]
  -> C.ConduitT BS.ByteString BS.ByteString m [(Word32, Word32, Value)]
readPage 0         _       = pure []
readPage remaining mb_dict = do
  pLogS "Reading a page"
  (page_header_size, page_header :: TT.PageHeader) <- decodeConduit remaining
  let
    page_content_size = page_header ^. TT.pinchField @"uncompressed_page_size"
  let
    validate_consumed_page_bytes consumed =
      unless (fromIntegral page_content_size == consumed)
        $ throwError "Reader did not consume the whole page!"
  let page_size = fromIntegral page_header_size + page_content_size
  case
      ( page_header ^. TT.pinchField @"dictionary_page_header"
      , page_header ^. TT.pinchField @"data_page_header"
      , mb_dict
      )
    of
      (Just dict_page_header, Nothing, Nothing) -> do
        (page_consumed, dict) <- dictPageReader dict_page_header
        validate_consumed_page_bytes page_consumed
        readPage (remaining - fromIntegral page_size) (Just dict)
      (Just _dict_page_header, Nothing, Just _dict) ->
        throwError "Found dictionary page while we already had a dictionary."
      (Nothing, Just dp_header, Nothing) -> do
        (page_consumed, page_result) <- dataPageReader dp_header Nothing
        validate_consumed_page_bytes page_consumed
        rest_page_results <- readPage
          (remaining - fromIntegral page_size)
          Nothing
        pure (rest_page_results ++ page_result)
      (Nothing, Just dp_header, Just dict) -> do
        (page_consumed, page_result) <- dataPageReader dp_header (Just dict)
        validate_consumed_page_bytes page_consumed
        rest_page_results <- readPage
          (remaining - fromIntegral page_size)
          Nothing
        pure (rest_page_results ++ page_result)
      (Nothing, Nothing, _) -> throwError
        "Page doesn't have any of the dictionary or data page header."
      (Just _, Just _, _) ->
        throwError "Page has both dictionary and data page headers."

readPageS
  :: (MonadReader PageCtx m, PR m)
  => Int64
  -> Maybe [Value]
  -> C.ConduitT BS.ByteString [(Word32, Word32, Value)] m ()
readPageS 0         _       = pure ()
readPageS remaining mb_dict = do
  pLogS "Reading a page!!!"
  (page_header_size, page_header :: TT.PageHeader) <- decodeConduit remaining
  let
    page_content_size = page_header ^. TT.pinchField @"uncompressed_page_size"
  let
    validate_consumed_page_bytes consumed =
      unless (fromIntegral page_content_size == consumed)
        $ throwError "Reader did not consume the whole page!"
  let page_size = fromIntegral page_header_size + page_content_size
  case
      ( page_header ^. TT.pinchField @"dictionary_page_header"
      , page_header ^. TT.pinchField @"data_page_header"
      , mb_dict
      )
    of
      (Just dict_page_header, Nothing, Nothing) -> do
        (page_consumed, dict) <- dictPageReader dict_page_header
        validate_consumed_page_bytes page_consumed
        readPageS (remaining - fromIntegral page_size) (Just dict)
      (Just _dict_page_header, Nothing, Just _dict) ->
        throwError "Found dictionary page while we already had a dictionary."
      (Nothing, Just dp_header, Nothing) -> do
        dataPageReaderS dp_header Nothing
      (Nothing, Just dp_header, Just dict) -> do
        dataPageReaderS dp_header (Just dict)
      (Nothing, Nothing, _) -> throwError
        "Page doesn't have any of the dictionary or data page header."
      (Just _, Just _, _) ->
        throwError "Page has both dictionary and data page headers."

failOnError :: Show err => IO (Either err b) -> IO b
failOnError v = v >>= \case
  Left  err -> fail $ show err
  Right val -> pure val

readMetadata :: Handle -> IO (Either T.Text TT.FileMetadata)
readMetadata h = do
  liftIO $ hSeek h SeekFromEnd (-8)
  !metadataSize <- liftIO $ run_handle_get BG.getWord32le
  liftIO $ hSeek h SeekFromEnd (-(8 + fromIntegral metadataSize))
  fmap (fmap (snd . fst))
    $            runExceptT
    $            C.runConduit
    $            C.sourceHandle h
    C..|         decodeConduit metadataSize
    `C.fuseBoth` pure ()
 where
  run_handle_get :: BG.Get a -> IO a
  run_handle_get get = do
    pos    <- hTell h
    !bytes <- LBS.hGetContents h
    case BG.runGetOrFail get bytes of
      Left too_much_info ->
        fail $ "Partial runGetOrFail failed: " <> show too_much_info
      Right (_, consumed, res) -> do
        hSeek h AbsoluteSeek $ fromIntegral (fromIntegral pos + consumed)
        pure res

decodeConduit
  :: forall a size m o
   . (MonadError T.Text m, MonadIO m, Integral size, Pinch.Pinchable a)
  => size
  -> C.ConduitT BS.ByteString o m (Int, a)
decodeConduit (fromIntegral -> size) = do
  (left, val) <-
    liftEither
    .   first T.pack
    .   Pinch.decodeWithLeftovers Pinch.compactProtocol
    .   LBS.toStrict
    =<< CB.take size
  C.leftover left
  pure (size - BS.length left, val)

decodeConduitS
  :: forall a size m
   . ( MonadError T.Text m
     , C.MonadThrow m
     , MonadIO m
     , Integral size
     , Pinch.Pinchable a
     )
  => size
  -> C.ConduitT BS.ByteString (Int, a) m ()
decodeConduitS (fromIntegral -> size) = do
  !bs         <- CB.sinkGet (BG.getByteString size)
  (left, val) <-
    liftEither
    . first T.pack
    . Pinch.decodeWithLeftovers Pinch.compactProtocol
    $ bs
  C.leftover left
  C.yield (size - BS.length left, val)
