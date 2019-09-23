module Parquet.Decoder where

import           Data.Bits
import qualified Data.ByteString as BS

decodeRLE :: Int -> BS.ByteString -> BS.ByteString
decodeRLE bit_width = id

decodeVarint :: BS.ByteString -> Integer
decodeVarint = go 0 0 . BS.unpack
  where
    go acc _ [] = acc
    go acc sh (byte:xs) =
      let
        high = byte .&. 0x80
        low = fromIntegral $ byte .&. 0x7F
        res = (low `shiftL` sh) .|. acc
      in
        if high == 0x80
          then go res (sh + 7) xs
          else res
