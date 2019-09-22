{-# LANGUAGE BangPatterns     #-}
{-# LANGUAGE LambdaCase       #-}
{-# LANGUAGE NamedFieldPuns   #-}
{-# LANGUAGE RankNTypes       #-}
{-# LANGUAGE TypeApplications #-}

module Parquet.Reader where

import qualified Data.Binary.Get       as BG
import qualified Data.ByteString.Char8 as BS
import qualified Data.ByteString.Lazy  as BL
import           Data.Foldable
import qualified Parquet.ThriftTypes   as TT
import qualified Pinch
import           System.IO
import           Text.Pretty.Simple

goRead :: Handle -> IO ()
goRead h = do
  metadata <- readMetadata h
  pPrint metadata
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
      hSeek h AbsoluteSeek (fromIntegral offset)
      let Pinch.Field size = TT._ColumnMetaData_total_compressed_size metadata
      print =<< decodeSized @TT.PageHeader h size
      pure ()


readMetadata :: Handle -> IO TT.FileMetadata
readMetadata h = do
  hSeek h SeekFromEnd (-8)
  !metadataSize <- runHandleGet h BG.getWord32le
  hSeek h SeekFromEnd (- (8 + fromIntegral metadataSize))
  decodeSized h metadataSize >>= \case
    Left err  -> fail err
    Right val -> pure val

decodeSized :: forall a size. (Integral size, Pinch.Pinchable a) => Handle -> size -> IO (Either String a)
decodeSized h size = do
  decode <$> runHandleGet h (BG.getByteString (fromIntegral size))

decode :: forall a. Pinch.Pinchable a => BS.ByteString -> Either String a
decode = Pinch.decode Pinch.compactProtocol

runHandleGet :: Handle -> BG.Get a -> IO a
runHandleGet h get = do
  bytes <- BL.hGetContents h
  return $ BG.runGet get bytes
