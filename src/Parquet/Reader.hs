{-# LANGUAGE BangPatterns     #-}
{-# LANGUAGE ScopedTypeVariables     #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE ConstraintKinds     #-}
{-# LANGUAGE FlexibleContexts     #-}
{-# LANGUAGE LambdaCase       #-}
{-# LANGUAGE RankNTypes       #-}
{-# LANGUAGE TypeApplications #-}

module Parquet.Reader where

import qualified Data.Binary.Get            as BG
import qualified Data.ByteString.Char8      as BS
import qualified Data.ByteString.Lazy       as LBS
import           Data.Foldable
import           Data.Functor               (($>))
import qualified Parquet.ThriftTypes        as TT
import qualified Pinch
import           System.IO
import           Text.Pretty.Simple
import           Text.Printf
import Control.Monad.Except
import Control.Monad.Reader
import qualified Data.Map as M
import Control.Arrow ((&&&))
import qualified Data.Text as T
import qualified Data.List.NonEmpty as NE
import Data.Bifunctor (first)
import Data.Traversable (for)
import Data.Word (Word8)
import Parquet.Decoder (decodeHybrid)
import Data.Int (Int32)

(<??>) :: MonadError b m => Maybe a -> b -> m a
(<??>) Nothing err = throwError err
(<??>) (Just v) _ = pure v

infixl 4 <??>

type PR m = (MonadReader PREnv m, MonadError T.Text m, MonadIO m)

newtype PREnv = PREnv { _prEnvSchema :: M.Map T.Text TT.SchemaElement }

goRead :: Handle -> ExceptT T.Text IO ()
goRead h = do
  metadata <- readMetadata h
  let Pinch.Field schema = TT._FileMetadata_schema metadata
  let schema_mapping = M.fromList $ map (TT.unField . TT._SchemaElement_name &&& id) schema
  let Pinch.Field row_groups = TT._FileMetadata_row_groups metadata
  let env = PREnv schema_mapping
  flip runReaderT env $ traverse_ (readRowGroup h) row_groups

readRowGroup :: (PR m) => Handle -> TT.RowGroup -> m ()
readRowGroup h row_group = do
  let Pinch.Field column_chunks = TT._RowGroup_column_chunks row_group
  traverse_ (readColumnChunk h) column_chunks

getRepType :: MonadError T.Text m => TT.SchemaElement -> m TT.FieldRepetitionType
getRepType elem =
  let
    Pinch.Field mb_rep_type = TT._SchemaElement_repetition_type elem
  in
    mb_rep_type <??> "Repetition type could not be found for elem " <> T.pack (show elem)

-- | Algorithm:
-- https://blog.twitter.com/engineering/en_us/a/2013/dremel-made-simple-with-parquet.html
calcMaxEncodingLevels :: PR m => NE.NonEmpty T.Text -> m (Word8, Word8)
calcMaxEncodingLevels path = do
  schema <- asks _prEnvSchema
  filled_path <- for path $ \name ->
                   M.lookup name schema <??> "Schema Element cannot be found: " <> name
  foldM (\(def, rep) elem -> do
      getRepType elem >>= \case
        (TT.REQUIRED _) ->
          pure (def, rep)
        (TT.OPTIONAL _) ->
          pure (def + 1, rep)
        (TT.REPEATED _) ->
          pure (def + 1, rep + 1)
    ) (0, 0) filled_path

readColumnChunk :: forall m. PR m => Handle -> TT.ColumnChunk -> m ()
readColumnChunk h cc = do
  let Pinch.Field offset = TT._ColumnChunk_file_offset cc

  -- Reading metadata
  let Pinch.Field mb_metadata = TT._ColumnChunk_meta_data cc
  metadata <- mb_metadata <??> "Metadata could not be found"
  -- pPrint metadata

  -- Reading page header
  liftIO $ hSeek h AbsoluteSeek (fromIntegral offset)

  let Pinch.Field size = TT._ColumnMetaData_total_compressed_size metadata
  page_header <- decodeSized @TT.PageHeader h size
  pPrint page_header
  let Pinch.Field page_size = TT._PageHeader_uncompressed_page_size page_header
  let Pinch.Field mb_dp_header = TT._PageHeader_data_page_header page_header
  case mb_dp_header of
    Nothing ->
      pure ()
    Just dp_header -> do
      let Pinch.Field rep_level_encoding = TT._DataPageHeader_repetition_level_encoding dp_header
      pPrint rep_level_encoding
      let Pinch.Field def_level_encoding = TT._DataPageHeader_definition_level_encoding dp_header
      let Pinch.Field path = TT._ColumnMetaData_path_in_schema metadata
      ne_path <- NE.nonEmpty path <??> "Schema path cannot be empty"
      (max_rep_level, max_def_level) <- calcMaxEncodingLevels ne_path
      read_rep page_size ne_path max_rep_level
      read_def ne_path max_def_level
  where
    read_rep :: Int32 -> NE.NonEmpty T.Text -> Word8 -> m ()
    read_rep size ne_path max_level =
      when (NE.length ne_path > 1) $ do
        liftIO $ pPrint =<< runHandleGet h (decodeHybrid max_level)
        pure ()

    read_def :: NE.NonEmpty T.Text -> Word8 -> m ()
    read_def ne_path max_level = do
      schema <- asks _prEnvSchema
      last_elem <- M.lookup (NE.head (NE.reverse ne_path)) schema <??> "Schema element could not be found"
      le_rep_ty <- getRepType last_elem
      case le_rep_ty of
        TT.OPTIONAL _ ->
          go
        TT.REPEATED _ ->
          go
        TT.REQUIRED _ ->
          pure ()
      where
        go = pure ()

failOnError :: Show err => IO (Either err b) -> IO b
failOnError v = v >>= \case
  Left err -> fail $ show err
  Right val -> pure val

readMetadata :: (MonadError T.Text m, MonadIO m) => Handle -> m TT.FileMetadata
readMetadata h = do
  liftIO $ hSeek h SeekFromEnd (-8)
  !metadataSize <- liftIO $ runHandleGet h BG.getWord32le
  liftIO $ hSeek h SeekFromEnd (- (8 + fromIntegral metadataSize))
  decodeSized h metadataSize

decodeSized :: forall a m size. (MonadError T.Text m, MonadIO m, Integral size, Pinch.Pinchable a) => Handle -> size -> m a
decodeSized h size = do
  pos <- liftIO $ hTell h
  !bs <- liftIO $ runHandleGet h $ BG.getByteString (fromIntegral size)
  case decode bs of
    Left err -> throwError err
    Right (leftovers, val) ->
      liftIO (hSeek h AbsoluteSeek (fromIntegral $ fromIntegral pos + BS.length bs - BS.length leftovers)) $> val

decode :: forall a. Pinch.Pinchable a => BS.ByteString -> Either T.Text (BS.ByteString, a)
decode = first T.pack . Pinch.decodeWithLeftovers Pinch.compactProtocol

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
