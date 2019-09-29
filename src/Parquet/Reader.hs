{-# LANGUAGE BangPatterns        #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RankNTypes          #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications    #-}

module Parquet.Reader where

import qualified Conduit               as C
import           Control.Arrow         ((&&&))
import           Control.Monad.Except
import           Control.Monad.Logger
import           Control.Monad.Reader
import           Data.Bifunctor        (first)
import qualified Data.Binary.Get       as BG
import qualified Data.ByteString.Char8 as BS
import qualified Data.ByteString.Lazy  as LBS
import           Data.Foldable
import qualified Data.Map              as M
import qualified Data.Text             as T
import           Parquet.Monad         (PR)
import           Parquet.PREnv
import           Parquet.Stream.Reader
import qualified Parquet.ThriftTypes   as TT
import qualified Pinch
import           System.IO

(<??>) :: MonadError b m => Maybe a -> b -> m a
(<??>) Nothing err = throwError err
(<??>) (Just v) _  = pure v

infixl 4 <??>

goRead :: Handle -> ExceptT T.Text IO ()
goRead h = do
  metadata <- readMetadata h
  let Pinch.Field schema = TT._FileMetadata_schema metadata
  let schema_mapping = M.fromList $ map (TT.unField . TT._SchemaElement_name &&& id) schema
  let Pinch.Field row_groups = TT._FileMetadata_row_groups metadata
  let env = PREnv schema_mapping
  runStdoutLoggingT $
    flip runReaderT env $
    traverse_ (readRowGroup h) row_groups

readRowGroup :: (MonadIO m, PR m) => Handle -> TT.RowGroup -> m ()
readRowGroup h row_group = do
  let Pinch.Field column_chunks = TT._RowGroup_column_chunks row_group
  traverse_ (readColumnChunk h) column_chunks

readColumnChunk :: forall m. (MonadIO m, PR m) => Handle -> TT.ColumnChunk -> m ()
readColumnChunk h cc = do
  let Pinch.Field offset = TT._ColumnChunk_file_offset cc

  let Pinch.Field mb_metadata = TT._ColumnChunk_meta_data cc
  metadata <- mb_metadata <??> "Metadata could not be found"

  let Pinch.Field compression = TT._ColumnMetaData_codec metadata
  case compression of
    TT.UNCOMPRESSED _ ->
      pure ()
    _ ->
      throwError "This library doesn't support compression algorithms yet."

  liftIO $ hSeek h AbsoluteSeek (fromIntegral offset)

  let Pinch.Field size = TT._ColumnMetaData_total_compressed_size metadata
  let Pinch.Field column_ty = TT._ColumnMetaData_type metadata
  let Pinch.Field dp_offset = TT._ColumnMetaData_data_page_offset metadata
  let Pinch.Field path = TT._ColumnMetaData_path_in_schema metadata

  liftIO $ hSeek h AbsoluteSeek (fromIntegral dp_offset)

  -- TODO(yigitozkavci): Transfer these decodeSized methods calls to Conduit
  -- as well.
  !page_header <- decodeSized @TT.PageHeader h size

  -- TODO(yigitozkavci): There are multiple pages in a column chunk.
  -- Read the rest of them. total_compressed_size has the size for all of
  -- the pages, that can be used to see when to end reading.
  !buf <- liftIO $ runHandleGet h $ BG.getByteString (fromIntegral size)
  !_page_content <- C.runConduit
                $ C.yield buf
             C..| readPage page_header path column_ty
     `C.fuseBoth` pure ()
  pure ()

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

-- | TODO: This is utterly inefficient. Fix it.
decodeSized :: forall a m size. (MonadError T.Text m, MonadIO m, Integral size, Pinch.Pinchable a) => Handle -> size -> m a
decodeSized h size = do
  pos <- liftIO $ hTell h
  !bs <- liftIO $ runHandleGet h $ BG.getByteString (fromIntegral size)
  case decode bs of
    Left err -> throwError err
    Right (leftovers, val) -> do
      let new_pos = fromIntegral $ fromIntegral pos + fromIntegral size - BS.length leftovers
      liftIO (hSeek h AbsoluteSeek new_pos)
      pure val

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
