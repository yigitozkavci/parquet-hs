{-# LANGUAGE BangPatterns     #-}
{-# LANGUAGE RankNTypes       #-}
{-# LANGUAGE TypeApplications #-}

module Parquet.Reader where

import qualified Data.Binary.Get       as BG
import qualified Data.ByteString.Char8 as BS
import qualified Data.ByteString.Lazy  as BL
import qualified Parquet.ThriftTypes   as TT
import qualified Pinch
import           System.IO

goRead :: Handle -> IO ()
goRead h =
  print =<< readMetadata h

readMetadata :: Handle -> IO (Either String TT.FileMetadata)
readMetadata file = do
  hSeek file SeekFromEnd (-8)
  !metadataSize <- runHandleGet file BG.getWord32le
  -- print metadataSize
  hSeek file SeekFromEnd (- (8 + fromIntegral metadataSize))
  metadataBs <- runHandleGet file (BG.getByteString (fromIntegral metadataSize))
  pure $ decode metadataBs

decode :: forall a. Pinch.Pinchable a => BS.ByteString -> Either String a
decode = Pinch.decode Pinch.compactProtocol

runHandleGet :: Handle -> BG.Get a -> IO a
runHandleGet h get = do
  bytes <- BL.hGetContents h
  return $ BG.runGet get bytes
