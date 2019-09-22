{-# LANGUAGE BangPatterns #-}

module Parquet.Reader where

import qualified Data.Binary.Get       as BG
import qualified Data.ByteString.Char8 as BS
import qualified Data.ByteString.Lazy  as BL
import           System.IO

goRead :: Handle -> IO ()
goRead file = do
  hSeek file SeekFromEnd (-8)
  !metadataSize <- runHandleGet file BG.getWord32le
  -- print metadataSize
  hSeek file SeekFromEnd (- (8 + fromIntegral metadataSize))
  print =<< runHandleGet file (BG.getByteString (fromIntegral metadataSize))

runHandleGet :: Handle -> BG.Get a -> IO a
runHandleGet h get = do
  bytes <- BL.hGetContents h
  return $ BG.runGet get bytes
