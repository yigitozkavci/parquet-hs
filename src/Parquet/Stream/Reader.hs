{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE TypeApplications #-}

module Parquet.Stream.Reader
  ( -- * Type definitions
    ColumnValue (..),
    Value (..),

    -- *
    decodeConduit,
    readColumnChunk,
  )
where

------------------------------------------------------------------------------

import qualified Conduit as C
import Control.Lens
import Control.Monad.Except
import Control.Monad.Logger (MonadLogger)
import Control.Monad.Logger.CallStack (logInfo)
import Control.Monad.Reader
import qualified Data.Binary.Get as BG
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as LBS
import qualified Data.Conduit.Binary as CB
import qualified Data.Conduit.Serialization.Binary as CB
import qualified Data.List.NonEmpty as NE
import qualified Data.Map as M
import qualified Data.Text as T (intercalate, pack)
import qualified Data.Text.Lazy as LT
import Data.Traversable (for)
import Parquet.Decoder (BitWidth (..), decodeBPBE, decodeRLEBPHybrid)
import Parquet.Monad
import Parquet.Prelude
import Parquet.Types as TT
import Parquet.Utils ((<??>))
import qualified Pinch
import Safe.Exact (zipExactMay)
import Text.Pretty.Simple (pString)

------------------------------------------------------------------------------

-- |
data ColumnValue = ColumnValue
  { _cvRepetitionLevel :: Word32,
    _cvDefinitionLevel :: Word32,
    _cvMaxDefinitionLevel :: Word32,
    _cvValue :: Value
  }
  deriving (Eq, Show)

------------------------------------------------------------------------------

-- |
data Value
  = Value_Int64 Int64
  | Value_ByteString ByteString
  | Value_Null
  deriving (Show, Eq)

------------------------------------------------------------------------------

-- |
data PageCtx = PageCtx
  { _pcSchema :: M.Map Text TT.SchemaElement,
    _pcPath :: NE.NonEmpty Text,
    _pcColumnTy :: TT.Type
  }
  deriving (Show, Eq)

------------------------------------------------------------------------------

-- | TODO: This is so unoptimized that my eyes bleed.
replicateMSized :: (Monad m) => Int -> m (Int64, result) -> m (Int64, [result])
replicateMSized 0 _ = pure (0, [])
replicateMSized n comp = do
  (consumed, result) <- comp
  (rest_consumed, rest_result) <- replicateMSized (n - 1) comp
  pure (consumed + rest_consumed, result : rest_result)

------------------------------------------------------------------------------

-- |
maxLevelToBitWidth :: Word8 -> BitWidth
maxLevelToBitWidth 0 = BitWidth 0
maxLevelToBitWidth max_level =
  BitWidth $ floor (logBase 2 (fromIntegral max_level) :: Double) + 1

------------------------------------------------------------------------------

-- |
dataPageReader ::
  forall m.
  (PR m, MonadReader PageCtx m) =>
  TT.DataPageHeader ->
  Maybe [Value] ->
  C.ConduitT ByteString ColumnValue m ()
dataPageReader header mb_dict = do
  let num_values = header ^. TT.pinchField @"num_values"
  let def_level_encoding = header ^. TT.pinchField @"definition_level_encoding"
  let rep_level_encoding = header ^. TT.pinchField @"repetition_level_encoding"
  let encoding = header ^. TT.pinchField @"encoding"
  (max_rep_level, max_def_level) <- calcMaxEncodingLevels
  (_rep_consumed, fill_level_default num_values -> rep_data) <-
    readRepetitionLevel
      rep_level_encoding
      (maxLevelToBitWidth max_rep_level)
      num_values
  (_def_consumed, fill_level_default num_values -> def_data) <-
    readDefinitionLevel
      def_level_encoding
      (maxLevelToBitWidth max_def_level)
      num_values
  level_data <- zip_level_data rep_data def_data
  path <- asks _pcPath
  logInfo $ LT.toStrict $ pString $ show path
  logInfo $ LT.toStrict $ pString $ show level_data
  read_page_content path encoding level_data num_values (fromIntegral max_def_level)
  pure ()
  where
    find_from_dict ::
      forall m0. MonadError Text m0 => [Value] -> Word32 -> m0 Value
    find_from_dict dict (fromIntegral -> d_index) = case dict ^? ix d_index of
      Nothing ->
        throwError $
          "A dictionary value couldn't be found in index "
            <> T.pack
              (show d_index)
      Just val -> pure val

    zip_level_data ::
      forall m0 a b. MonadError Text m0 => [a] -> [b] -> m0 [(a, b)]
    zip_level_data rep_data def_data =
      zipExactMay rep_data def_data
        <??> ( "Repetition and Definition data sizes differ: page has "
                 <> T.pack (show (length rep_data))
                 <> " repetition values and "
                 <> T.pack (show (length def_data))
                 <> " definition values."
             )

    fill_level_default :: Int32 -> [Word32] -> [Word32]
    fill_level_default num_values = \case
      [] -> replicate (fromIntegral num_values) 0
      xs -> xs

    read_page_content ::
      NE.NonEmpty Text ->
      TT.Encoding ->
      [(Word32, Word32)] ->
      Int32 ->
      Word32 ->
      C.ConduitT ByteString ColumnValue m ()
    read_page_content path encoding level_data num_values max_def_level =
      case (mb_dict, encoding) of
        (Nothing, TT.PLAIN _) -> do
          vals <- for level_data $ \(r, d) ->
            if
                | d == max_def_level -> do
                  (_, val) <- decodeValue
                  pure (ColumnValue r d max_def_level val)
                | otherwise -> pure $ ColumnValue r d max_def_level Value_Null
          logInfo $ "Values (" <> T.pack (show path) <> "): " <> (LT.toStrict $ pString $ show vals)
          C.yieldMany vals
        (Just _, TT.PLAIN _) ->
          throwError
            "We shouldn't have PLAIN-encoded data pages with a dictionary."
        (Just dict, TT.PLAIN_DICTIONARY _) -> do
          !bit_width <- CB.sinkGet BG.getWord8
          val_indexes <-
            CB.sinkGet $
              decodeRLEBPHybrid (BitWidth bit_width) num_values
          vals <- construct_dict_values max_def_level dict level_data val_indexes
          logInfo $ "Values: " <> (LT.toStrict $ pString $ show vals)
          C.yieldMany vals
        (Nothing, TT.PLAIN_DICTIONARY _) ->
          throwError
            "Data page has PLAIN_DICTIONARY encoding but we don't have a dictionary yet."
        other ->
          throwError $
            "Don't know how to encode data pages with encoding: "
              <> T.pack (show other)
    construct_dict_values ::
      forall m0.
      (MonadError Text m0) =>
      Word32 ->
      [Value] ->
      [(Word32, Word32)] ->
      [Word32] ->
      m0 [ColumnValue]
    construct_dict_values _ _ [] _ = pure []
    construct_dict_values _ _ _ [] =
      throwError
        "There are not enough level data for given amount of dictionary indexes."
    construct_dict_values max_def_level dict ((r, d) : lx) (v : vx)
      | d == max_def_level = do
        val <- find_from_dict dict v
        (ColumnValue r d max_def_level val :)
          <$> construct_dict_values max_def_level dict lx vx
      | otherwise = do
        (ColumnValue r d max_def_level Value_Null :)
          <$> construct_dict_values max_def_level dict lx (v : vx)

------------------------------------------------------------------------------

-- |
decodeValue ::
  (PR m, MonadReader PageCtx m) =>
  C.ConduitT ByteString o m (Int64, Value)
decodeValue =
  asks _pcColumnTy >>= \case
    (TT.BYTE_ARRAY _) -> do
      !len <- CB.sinkGet BG.getWord32le
      (consumed, result) <-
        replicateMSized
          (fromIntegral len)
          (CB.sinkGet (sizedGet BG.getWord8))
      pure (consumed + 4, Value_ByteString (BS.pack result))
    (TT.INT64 _) -> do
      (consumed, result) <- CB.sinkGet (sizedGet BG.getInt64le)
      pure (consumed, Value_Int64 result)
    ty ->
      throwError $
        "Don't know how to decode value of type "
          <> T.pack (show ty)
          <> " yet."

------------------------------------------------------------------------------

-- |
dictPageReader ::
  (MonadReader PageCtx m, PR m) =>
  TT.DictionaryPageHeader ->
  C.ConduitT ByteString o m (Int64, [Value])
dictPageReader header = do
  let num_values = header ^. TT.pinchField @"num_values"
  let _encoding = header ^. TT.pinchField @"encoding"
  let _is_sorted = header ^. TT.pinchField @"is_sorted"
  (consumed, vals) <- replicateMSized (fromIntegral num_values) decodeValue
  pure (consumed, vals)

getLastSchemaElement ::
  (MonadError Text m, MonadReader PageCtx m) => m TT.SchemaElement
getLastSchemaElement = do
  path <- asks _pcPath
  schema <- asks _pcSchema
  M.lookup (T.intercalate "." (NE.toList path)) schema
    <??> "Schema element could not be found"

------------------------------------------------------------------------------

-- |
readDefinitionLevel ::
  (PR m, MonadReader PageCtx m) =>
  TT.Encoding ->
  BitWidth ->
  Int32 ->
  C.ConduitT ByteString a m (Int64, [Word32])
readDefinitionLevel _ (BitWidth 0) _ = pure (0, [])
readDefinitionLevel encoding bit_width num_values =
  getLastSchemaElement >>= getRepType >>= \case
    TT.OPTIONAL _ -> decodeLevel encoding bit_width num_values
    TT.REPEATED _ -> decodeLevel encoding bit_width num_values
    TT.REQUIRED _ -> pure (0, [])

------------------------------------------------------------------------------

-- |
readRepetitionLevel ::
  (C.MonadThrow m, MonadError Text m, MonadReader PageCtx m, MonadLogger m) =>
  TT.Encoding ->
  BitWidth ->
  Int32 ->
  C.ConduitT ByteString a m (Int64, [Word32])
readRepetitionLevel encoding bit_width num_values = do
  decodeLevel encoding bit_width num_values

------------------------------------------------------------------------------

-- |
sizedGet :: BG.Get result -> BG.Get (Int64, result)
sizedGet g = do
  (before, result, after) <- liftA3 (,,) BG.bytesRead g BG.bytesRead
  pure (after - before, result)

------------------------------------------------------------------------------

-- |
decodeLevel ::
  (C.MonadThrow m, MonadError Text m, MonadLogger m, MonadReader PageCtx m) =>
  TT.Encoding ->
  BitWidth ->
  Int32 ->
  C.ConduitT ByteString a m (Int64, [Word32])
decodeLevel _ (BitWidth 0) _ = pure (0, [])
decodeLevel encoding bit_width (fromIntegral -> num_values) = case encoding of
  TT.RLE _ ->
    CB.sinkGet $
      sizedGet $
        BG.getWord32le
          *> decodeRLEBPHybrid bit_width num_values
  TT.BIT_PACKED _ ->
    CB.sinkGet $
      sizedGet $
        BG.getWord32le
          *> (take (fromIntegral num_values) <$> decodeBPBE bit_width)
  _ -> throwError "Only RLE and BIT_PACKED encodings are supported for levels"

------------------------------------------------------------------------------

-- | Algorithm:
-- https://blog.twitter.com/engineering/en_us/a/2013/dremel-made-simple-with-parquet.html
calcMaxEncodingLevels ::
  forall m.
  (MonadReader PageCtx m, MonadError Text m) =>
  m (Word8, Word8)
calcMaxEncodingLevels = do
  schema <- asks _pcSchema
  path <- asks _pcPath
  calc_max_encoding_levels schema path
  where
    calc_max_encoding_levels :: M.Map Text TT.SchemaElement -> NE.NonEmpty Text -> m (Word8, Word8)
    calc_max_encoding_levels schema path = do
      let pathText = T.intercalate "." (NE.toList path)
      schema_element <- M.lookup pathText schema <??> ("Schema Element cannot be found: " <> pathText)
      (repVal, defVal) <-
        getRepType schema_element >>= \case
          (TT.REQUIRED _) -> pure (0, 0)
          (TT.OPTIONAL _) -> pure (0, 1)
          (TT.REPEATED _) -> pure (1, 1)
      case NE.init path of
        (root : v : vx) -> bimap (+ repVal) (+ defVal) <$> calc_max_encoding_levels schema (root NE.:| v : vx)
        _ -> pure (repVal, defVal)

------------------------------------------------------------------------------

-- |
getRepType ::
  MonadError Text m => TT.SchemaElement -> m TT.FieldRepetitionType
getRepType e =
  e
    ^. TT.pinchField @"repetition_type"
    <??> "Repetition type could not be found for elem "
    <> T.pack (show e)

------------------------------------------------------------------------------

-- |
validateCompression :: MonadError Text m => TT.ColumnMetaData -> m ()
validateCompression metadata =
  let compression = metadata ^. TT.pinchField @"codec"
   in case compression of
        TT.UNCOMPRESSED _ -> pure ()
        _ ->
          throwError "This library doesn't support compression algorithms yet."

------------------------------------------------------------------------------

-- |
readColumnChunk ::
  PR m =>
  TT.SchemaElement ->
  M.Map Text TT.SchemaElement ->
  TT.ColumnChunk ->
  C.ConduitT ByteString ColumnValue m ()
readColumnChunk root schema cc = do
  let mb_metadata = cc ^. TT.pinchField @"meta_data"
  metadata <- mb_metadata <??> "Metadata could not be found"
  validateCompression metadata
  let size = metadata ^. TT.pinchField @"total_compressed_size"
  let column_ty = metadata ^. TT.pinchField @"type"
  let path = root ^. TT.pinchField @"name" NE.:| metadata ^. TT.pinchField @"path_in_schema"
  let page_ctx = PageCtx schema path column_ty
  C.runReaderC page_ctx $ readPage size Nothing

------------------------------------------------------------------------------

-- |
readPage ::
  (MonadLogger m, MonadReader PageCtx m, PR m) =>
  Int64 ->
  Maybe [Value] ->
  C.ConduitT ByteString ColumnValue m ()
readPage 0 _ = pure ()
readPage remaining mb_dict = do
  (page_header_size, page_header :: TT.PageHeader) <- decodeConduit remaining
  logInfo $ LT.toStrict $ pString $ show page_header
  let page_content_size = page_header ^. TT.pinchField @"uncompressed_page_size"
  let validate_consumed_page_bytes consumed =
        unless (fromIntegral page_content_size == consumed) $
          throwError "Reader did not consume the whole page!"
  let page_size = fromIntegral page_header_size + page_content_size
  case ( page_header ^. TT.pinchField @"dictionary_page_header",
         page_header ^. TT.pinchField @"data_page_header",
         mb_dict
       ) of
    (Just dict_page_header, Nothing, Nothing) -> do
      (page_consumed, dict) <- dictPageReader dict_page_header
      validate_consumed_page_bytes page_consumed
      readPage (remaining - fromIntegral page_size) (Just dict)
    (Just _dict_page_header, Nothing, Just _dict) ->
      throwError "Found dictionary page while we already had a dictionary."
    (Nothing, Just dp_header, Nothing) -> do
      dataPageReader dp_header Nothing
    (Nothing, Just dp_header, Just dict) -> do
      dataPageReader dp_header (Just dict)
    (Nothing, Nothing, _) ->
      throwError
        "Page doesn't have any of the dictionary or data page header."
    (Just _, Just _, _) ->
      throwError "Page has both dictionary and data page headers."

------------------------------------------------------------------------------

-- |
decodeConduit ::
  forall a size m o.
  (MonadError Text m, MonadIO m, Integral size, Pinch.Pinchable a) =>
  size ->
  C.ConduitT ByteString o m (Int, a)
decodeConduit (fromIntegral -> size) = do
  (left, val) <-
    liftEither
      . first T.pack
      . Pinch.decodeWithLeftovers Pinch.compactProtocol
      . LBS.toStrict
      =<< CB.take size
  C.leftover left
  pure (size - BS.length left, val)
