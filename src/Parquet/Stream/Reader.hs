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
import Control.Applicative (liftA3)
import Control.Monad.Except
import Control.Monad.Logger (MonadLogger)
import Control.Monad.Logger.CallStack
import Control.Monad.Reader
import Data.Bifunctor (first)
import qualified Data.Binary.Get as BG
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as LBS
import qualified Data.Conduit.Binary as CB
import qualified Data.Conduit.Serialization.Binary as CB
import Data.Int (Int32, Int64)
import qualified Data.List.NonEmpty as NE
import qualified Data.Map as M
import qualified Data.Text as T
import Data.Traversable (for)
import Data.Word (Word32, Word8)
import Control.Lens
import qualified Pinch
import Safe.Exact (zipExactMay)
import qualified Codec.Compression.Snappy as Snappy
import qualified Data.Sequence as Seq

import Parquet.Decoder (BitWidth(..), decodeBPBE, decodeRLEBPHybrid)
import Parquet.Monad
import qualified Parquet.ThriftTypes as TT
import Parquet.Utils ((<??>))

data ColumnValue = ColumnValue
  { _cvRepetitionLevel :: Word32
  , _cvDefinitionLevel :: Word32
  , _cvMaxDefinitionLevel :: Word32
  , _cvValue :: Value
  } deriving (Eq, Show)

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

dataPageReader
  :: forall m
   . (PR m, MonadReader PageCtx m)
  => TT.DataPageHeader
  -> Maybe [Value]
  -> C.ConduitT BS.ByteString ColumnValue m Int64
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
  level_data    <- zip_level_data rep_data def_data
  page_consumed <- read_page_content
    encoding
    level_data
    num_values
    (fromIntegral max_def_level)
    (fromIntegral max_rep_level)
  pure $ rep_consumed + def_consumed + page_consumed
 where
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
    [] -> replicate (fromIntegral num_values) 0
    xs -> xs

  read_page_content
    :: TT.Encoding
    -> [(Word32, Word32)]
    -> Int32
    -> Word32
    -> Word32
    -> C.ConduitT BS.ByteString ColumnValue m Int64
  read_page_content encoding level_data num_values max_def_level max_rep_level
    = case (mb_dict, encoding) of
      (Nothing, TT.PLAIN _) -> do
        logError $ "(1) NODICT"
        (consumed, vals) <- foldM
          (\(i_consumed, vals) (r, d) -> if d == max_def_level
            then do
              (consumed, val) <- decodeValue
              pure
                ( i_consumed + consumed
                , vals Seq.|> ColumnValue r d max_def_level val
                )
            else pure
              (i_consumed, vals Seq.|> ColumnValue r d max_def_level Null)
          )
          (0, Seq.empty)
          level_data
        C.yieldMany vals
        logError $ "(2) NODICT"
        pure consumed
      (Just _, TT.PLAIN _) -> throwError
        "We shouldn't have PLAIN-encoded data pages with a dictionary."
      (Just dict, TT.PLAIN_DICTIONARY _) -> do
        logError $ "(1) DICT"
        !bit_width              <- CB.sinkGet BG.getWord8
        -- val_indexes <-
        --   traverse
        --       (\(_, v) -> case v of
        --         ValueInt64 i -> pure (fromIntegral i)
        --         ValueInt32 i -> pure (fromIntegral i)
        --       )
        --     =<< replicateM (fromIntegral num_values) decodeValue
        (consumed, val_indexes) <- CB.sinkGet $ sizedGet $ decodeRLEBPHybrid
          (BitWidth bit_width)
          num_values

        logError $ "(2) Level length: " <> T.pack (show (length level_data))
        logError $ "(2) Index length: " <> T.pack (show (length val_indexes))
        logError $ "(2) Numvals: " <> T.pack (show num_values)
        vals <- construct_dict_values
          max_def_level
          max_rep_level
          dict
          level_data
          val_indexes
        C.yieldMany vals
        pure consumed
      (Nothing, TT.PLAIN_DICTIONARY _) -> do
        path <- asks _pcPath
        throwError
          $ "Data page has PLAIN_DICTIONARY encoding but we don't have a dictionary yet."
          <> T.pack (show path)
      other ->
        throwError
          $  "Don't know how to encode data pages with encoding: "
          <> T.pack (show other)

  -- | Given repetition and definition level data, a dictionary and a set of indexes,
  -- constructs values for this dictionary-encoded page.
  construct_dict_values
    :: forall m0
     . (MonadError T.Text m0, MonadLogger m0)
    => Word32
    -> Word32
    -> [Value]
    -> [(Word32, Word32)]
    -> [Word32]
    -> m0 [ColumnValue]
  construct_dict_values _ _ _ [] _ = pure []
  construct_dict_values _ _ dict levels [] =
    []
      <$ (  logError ("Skipping a dict. Dict: " <> T.pack (show (length dict)))
         *> logError
              ("Skipping a dict. Levels: " <> T.pack (show (length levels)))
         )
    -- throwError
    --   $ "There are not enough level data for given amount of dictionary indexes."
    --   <> T.pack (show p)
  construct_dict_values max_def_level max_rep_level dict ((r, d) : lx) (v : vx)
    | d == max_def_level = do
      val <- find_from_dict dict v
      -- logError
      --   $  "Reducing: r="
      --   <> T.pack (show r)
      --   <> ", d="
      --   <> T.pack (show d)
      --   <> ", max_def_level="
      --   <> T.pack (show max_def_level)
      --   <> ", val="
      --   <> T.pack (show val)
      (ColumnValue r d max_def_level val :)
        <$> construct_dict_values max_def_level max_rep_level dict lx vx
    | otherwise = do
      -- logError
      --   $  "Otherwise: r="
      --   <> T.pack (show r)
      --   <> ", d="
      --   <> T.pack (show d)
      --   <> ", max_def_level="
      --   <> T.pack (show max_def_level)
      (ColumnValue r d max_def_level Null :)
        <$> construct_dict_values max_def_level max_rep_level dict lx (v : vx)

  find_from_dict
    :: forall  m0 . MonadError T.Text m0 => [Value] -> Word32 -> m0 Value
  find_from_dict dict (fromIntegral -> d_index) = case dict ^? ix d_index of
    Nothing ->
      throwError $ "A dictionary value couldn't be found in index " <> T.pack
        (show d_index)
    Just val -> pure val

data Value
  = ValueInt64 Int64
  | ValueInt32 Int32
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
  (TT.INT32 _) -> do
    (consumed, result) <- CB.sinkGet (sizedGet BG.getInt32le)
    pure (consumed, ValueInt32 result)
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
    , _pcColumnCodec :: TT.CompressionCodec
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
  decodeLevel encoding bit_width num_values

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

validateCompression :: MonadError T.Text m => TT.ColumnMetaData -> m ()
validateCompression metadata =
  let compression = metadata ^. TT.pinchField @"codec"
  in
    case compression of
      TT.UNCOMPRESSED _ -> pure ()
      _ ->
        throwError "This library doesn't support compression algorithms yet."

readColumnChunk
  :: forall m
   . PR m
  => M.Map T.Text TT.SchemaElement
  -> TT.ColumnChunk
  -> C.ConduitT BS.ByteString ColumnValue m ()
readColumnChunk schema cc = do
  let mb_metadata = cc ^. TT.pinchField @"meta_data"
  metadata <- mb_metadata <??> "Metadata could not be found"
  -- validateCompression metadata
  page_ctx <- mk_page_ctx metadata
  let column_chunk_size = metadata ^. TT.pinchField @"total_uncompressed_size"
  logError $ T.pack $ show metadata
  read_column_chunk_contents page_ctx column_chunk_size Nothing
 where
  mk_page_ctx
    :: TT.ColumnMetaData -> C.ConduitT BS.ByteString ColumnValue m PageCtx
  mk_page_ctx metadata = do
    let column_ty    = metadata ^. TT.pinchField @"type"
    let path         = metadata ^. TT.pinchField @"path_in_schema"
    let column_codec = metadata ^. TT.pinchField @"codec"
    ne_path <- NE.nonEmpty path <??> "Schema path cannot be empty"
    pure $ PageCtx schema ne_path column_ty column_codec

  read_column_chunk_contents
    :: PageCtx
    -> Int64
    -> Maybe [Value]
    -> C.ConduitT BS.ByteString ColumnValue m ()
  read_column_chunk_contents _ 0 mb_dict =
    logError "Finished reading a column chunk!!!"
  read_column_chunk_contents page_ctx remaining mb_dict = do
    logError
      $  "Reading a column chunk"
      <> (T.pack $ show $ _pcPath page_ctx)
      <> ", rem_size: "
      <> T.pack (show remaining)
    (consumed, new_mb_dict) <- C.runReaderC page_ctx
      $ readPage remaining mb_dict
    read_column_chunk_contents page_ctx (remaining - consumed) new_mb_dict

readPage
  :: forall m
   . (MonadReader PageCtx m, PR m)
  => Int64
  -> Maybe [Value]
  -> C.ConduitT BS.ByteString ColumnValue m (Int64, Maybe [Value])
readPage column_chunk_size mb_dict = do
   -- FIXME(yigitozkavci): should not use the whole column chunk size for reading a single page's header.
  (page_header_size, page_header) <- read_page_header
  let page_size = page_header ^. TT.pinchField @"uncompressed_page_size"
  path <- asks _pcPath
  logError
    $  "Read page header. Path: "
    <> T.pack (show path)
    <> ", Size:"
    <> T.pack (show page_size)
    <> ", Page Header Size:"
    <> T.pack (show page_header_size)
    <> ", Chunk Size:"
    <> T.pack (show column_chunk_size)
    <> ", PageHeader:"
    <> T.pack (show page_header)
  new_mb_dict <- read_page_contents page_header page_size mb_dict
  pure (fromIntegral page_header_size + fromIntegral page_size, new_mb_dict)
 where
  read_page_header :: C.ConduitT BS.ByteString o m (Int, TT.PageHeader)
  read_page_header = do
    path <- asks _pcPath
    logError $ "Reading a page header" <> T.pack (show path)
    decodeConduit column_chunk_size

  read_page_contents
    :: TT.PageHeader
    -> Int32
    -> Maybe [Value]
    -> C.ConduitT BS.ByteString ColumnValue m (Maybe [Value])
  read_page_contents _ 0 mb_dict = do
    logError "Finished reading a page!!!"
    pure mb_dict
  read_page_contents page_header remaining_page_size mb_dict = do
    path <- asks _pcPath
    logError
      $  "Reading a page content. Remaining: "
      <> T.pack (show remaining_page_size)
      <> ", Path: "
      <> T.pack (show path)
    let
      uncompressed_page_size =
        page_header ^. TT.pinchField @"uncompressed_page_size"
      validate_consumed_page_bytes ty consumed =
        unless (fromIntegral uncompressed_page_size == consumed)
          $  throwError
          $  T.pack (show ty)
          <> " Reader did not consume the whole page! Size: "
          <> T.pack (show remaining_page_size)
          <> " Consumed: "
          <> T.pack (show consumed)
    decompressStream $ page_header ^. TT.pinchField @"compressed_page_size"
    case
        ( page_header ^. TT.pinchField @"dictionary_page_header"
        , page_header ^. TT.pinchField @"data_page_header"
        , mb_dict
        )
      of
        (Just dict_page_header, Nothing, Nothing) -> do
          logError . ("Reading a DICT page" <>) . T.pack . show =<< asks _pcPath
          (page_consumed, dict) <- dictPageReader dict_page_header
          validate_consumed_page_bytes "Dict Page" page_consumed -- Dict pages are required to be fully consumed
          path <- asks _pcPath
          logError
            $  "Read a DICT page. Path: "
            <> T.pack (show path)
            <> ", Remaining: "
            <> T.pack (show remaining_page_size)
            <> ", Consumed: "
            <> T.pack (show page_consumed)
          read_page_contents
            page_header
            (remaining_page_size - fromIntegral page_consumed)
            (Just dict)
        (Just _dict_page_header, Nothing, Just _dict) ->
          throwError "Found dictionary page while we already had a dictionary."
        (Nothing, Just dp_header, Nothing) -> do
          logError . ("Reading a DATA page" <>) . T.pack . show =<< asks _pcPath
          page_consumed <- dataPageReader dp_header Nothing
          -- validate_consumed_page_bytes "Data Page" consumed
          read_page_contents
            page_header
            (remaining_page_size - fromIntegral page_consumed)
            Nothing
        (Nothing, Just dp_header, Just dict) -> do
          logError
            .   ("Reading a DATA_DICT page" <>)
            .   T.pack
            .   show
            =<< asks _pcPath
          page_consumed <- dataPageReader dp_header (Just dict)
          logError
            $  "Read a DATA_DICT page. Path: "
            <> T.pack (show path)
            <> ", Remaining: "
            <> T.pack (show remaining_page_size)
            <> ", Consumed: "
            <> T.pack (show page_consumed)
          -- validate_consumed_page_bytes "Data Page" page_consumed
          read_page_contents
            page_header
            (remaining_page_size - fromIntegral page_consumed)
            (Just dict)
        (Nothing, Nothing, _) -> throwError
          "Page doesn't have any of the dictionary or data page header."
        (Just _, Just _, _) ->
          throwError "Page has both dictionary and data page headers."

decompressStream size = do
  codec <- asks _pcColumnCodec
  case codec of
    TT.SNAPPY _ -> C.leftover =<< Snappy.decompress . BS.pack <$> replicateM
      (fromIntegral size)
      (CB.sinkGet BG.getWord8)
    _ -> pure ()

failOnError :: Show err => IO (Either err b) -> IO b
failOnError v = v >>= \case
  Left  err -> fail $ show err
  Right val -> pure val

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
