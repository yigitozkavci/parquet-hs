{-# LANGUAGE BangPatterns     #-}
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

goRead :: Handle -> IO ()
goRead h = do
  metadata <- readMetadata h
  let Pinch.Field row_groups = TT._FileMetadata_row_groups metadata
  traverse_ (readRowGroup h) row_groups

readRowGroup :: Handle -> TT.RowGroup -> IO ()
readRowGroup h row_group = do
  let Pinch.Field column_chunks = TT._RowGroup_column_chunks row_group
  traverse_ (readColumnChunk h) column_chunks

readColumnChunk :: Handle -> TT.ColumnChunk -> IO ()
readColumnChunk h cc = do
  let Pinch.Field offset = TT._ColumnChunk_file_offset cc
  let Pinch.Field mb_metadata = TT._ColumnChunk_meta_data cc
  case mb_metadata of
    Nothing ->
      fail "Wow no metadata"
    Just metadata -> do
      pPrint metadata
      hSeek h AbsoluteSeek (fromIntegral offset)
      let Pinch.Field size = TT._ColumnMetaData_total_compressed_size metadata
      page_header <- failOnError $ decodeSized @TT.PageHeader h size
      pPrint page_header
      let Pinch.Field _page_size = TT._PageHeader_uncompressed_page_size page_header
      -- !len <- runHandleGet h BG.getWord32le
      -- printf "%d\n" len
      !w <- runHandleGet h BG.getWord8
      printf "%b\n" w
      -- pure ()

failOnError :: Show err => IO (Either err b) -> IO b
failOnError v = v >>= \case
  Left err -> fail $ show err
  Right val -> pure val

readMetadata :: Handle -> IO TT.FileMetadata
readMetadata h = do
  hSeek h SeekFromEnd (-8)
  !metadataSize <- runHandleGet h BG.getWord32le
  hSeek h SeekFromEnd (- (8 + fromIntegral metadataSize))
  failOnError $ decodeSized h metadataSize

decodeSized :: forall a size. (Integral size, Pinch.Pinchable a) => Handle -> size -> IO (Either String a)
decodeSized h size = do
  pos <- hTell h
  !bs <- runHandleGet h $ BG.getByteString (fromIntegral size)
  case decode bs of
    Left err -> pure $ Left err
    Right (leftovers, val) ->
      hSeek h AbsoluteSeek (fromIntegral $ fromIntegral pos + BS.length bs - BS.length leftovers) $> Right val

decode :: forall a. Pinch.Pinchable a => BS.ByteString -> Either String (BS.ByteString, a)
decode = Pinch.decodeWithLeftovers Pinch.compactProtocol

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
