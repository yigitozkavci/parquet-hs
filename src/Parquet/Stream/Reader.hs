{-# LANGUAGE BangPatterns      #-}
{-# LANGUAGE FlexibleContexts  #-}
{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE OverloadedStrings #-}

module Parquet.Stream.Reader where

import qualified Conduit                           as C
import           Control.Monad.Except
import           Control.Monad.Logger
import           Control.Monad.Reader
import qualified Data.Binary.Get                   as BG
import qualified Data.ByteString                   as BS
import qualified Data.Conduit.Serialization.Binary as C
import qualified Data.List.NonEmpty                as NE
import qualified Data.Map                          as M
import qualified Data.Text                         as T
import qualified Data.Text.Lazy                    as TL
import           Data.Traversable                  (for)
import           Data.Word                         (Word32, Word64, Word8)
import           Parquet.Decoder                   (decodeBPBE, decodeHybrid)
import           Parquet.Monad
import           Parquet.PREnv                     (PREnv (..))
import qualified Parquet.ThriftTypes               as TT
import           Parquet.Utils                     ((<??>))
import qualified Pinch
import           Text.Pretty.Simple                (pShow)

pLog :: (MonadLogger m, Show a) => a -> m ()
pLog = logInfoN . TL.toStrict . pShow

dataPageReader
  :: PR m
  => TT.DataPageHeader
  -> TT.Type
  -> NE.NonEmpty T.Text
  -> C.ConduitT BS.ByteString BS.ByteString m [Word8]
dataPageReader dp_header column_ty ne_path = do
  let Pinch.Field _rep_level_encoding = TT._DataPageHeader_repetition_level_encoding dp_header
  let Pinch.Field num_values         = TT._DataPageHeader_num_values                dp_header
  let Pinch.Field _def_level_encoding = TT._DataPageHeader_definition_level_encoding dp_header
  _schema <- asks _prEnvSchema
  (max_rep_level, max_def_level) <- calcMaxEncodingLevels ne_path
  pLog column_ty
  _rep_data <- readRepetitionLevel ne_path max_rep_level
  _def_data <- readDefinitionLevel ne_path max_def_level
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
  => TT.PageHeader
  -> [T.Text]
  -> TT.Type
  -> C.ConduitT BS.ByteString BS.ByteString m [Word8]
readPage page_header path column_ty = do
  ne_path <- NE.nonEmpty path <??> "Schema path cannot be empty"
  let Pinch.Field page_size = TT._PageHeader_uncompressed_page_size page_header
  let Pinch.Field mb_dp_header = TT._PageHeader_data_page_header page_header
  buf <- C.sinkGet (BG.getByteString (fromIntegral page_size))
  case mb_dp_header of
    Nothing ->
      return []
    Just dp_header ->
      C.yield buf C..| dataPageReader dp_header column_ty ne_path

readDefinitionLevel
  :: PR m
  => NE.NonEmpty T.Text
  -> Word8
  -> C.ConduitT BS.ByteString BS.ByteString m [Word32]
readDefinitionLevel _ 0 = pure []
readDefinitionLevel ne_path max_level = do
  schema <- asks _prEnvSchema
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
  :: (C.MonadThrow m, MonadError T.Text m, MonadReader PREnv m)
  => NE.NonEmpty T.Text -> Word8 -> C.ConduitT BS.ByteString BS.ByteString m [Word32]
readRepetitionLevel ne_path max_level
  | max_level > 0 && NE.length ne_path > 1 =
    C.sinkGet (decodeBPBE max_level)
  | otherwise = pure []

-- | Algorithm:
-- https://blog.twitter.com/engineering/en_us/a/2013/dremel-made-simple-with-parquet.html
calcMaxEncodingLevels :: (MonadReader PREnv m, MonadError T.Text m) => NE.NonEmpty T.Text -> m (Word8, Word8)
calcMaxEncodingLevels path = do
  schema <- asks _prEnvSchema
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
