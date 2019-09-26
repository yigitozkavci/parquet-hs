module Parquet.Decoder where

import           Data.Binary.Get
import           Data.Bits
import qualified Data.ByteString as BS
import           Data.Word       (Word32, Word8)
import Control.Monad
import Text.Printf
import Debug.Trace

cLeb128ByteLimit :: Num a => a
cLeb128ByteLimit = 32

takeBytesLe :: Word8 -> Get Integer
takeBytesLe 0 = pure 0
takeBytesLe n = do
  v <- getWord8
  rest <- takeBytesLe (n - 1)
  pure $ (rest `shiftL` 8) .|. fromIntegral v

takeBytesBe :: Int -> Word8 -> Get Integer
takeBytesBe = go
  where
    go :: Int -> Word8 -> Get Integer
    go sh 0 = pure 0
    go sh n = do
      v <- getWord8
      rest <- go (sh - 1) (n - 1)
      pure $ (fromIntegral v `shiftL` (8 * (sh - 1))) .|. rest

decodeBPBE :: Word8 -> Word32 -> Get [Word32]
decodeBPBE bit_width 0 = pure []
decodeBPBE bit_width scaled_run_len = do
  v <- takeBytesBe (fromIntegral bit_width) bit_width
  batch_bytes <- go 8 v
  (batch_bytes <>) <$> decodeBPBE bit_width (scaled_run_len - 1)
  where
    go :: Int -> Integer -> Get [Word32]
    go 0 data_bytes = pure []
    go rem_vals data_bytes = do
      let mask :: Integer
          mask = ((2 ^ bit_width) - 1) `shiftL` (fromIntegral bit_width * 7)
      -- Unsafe fromInteger justification:
      -- Max bit_width = 32 and masking any value with
      -- (2 ^ 32) - 1 is in unsigned 32-bit bound.
      let val = fromInteger $ (data_bytes .&. mask) `shiftR` (fromIntegral bit_width * 7)
      rest <- go (rem_vals - 1) (data_bytes `shiftL` fromIntegral bit_width)
      pure $ val:rest

decodeBPLE :: Word8 -> Word32 -> Get [Word32]
decodeBPLE bit_width 0 = pure []
decodeBPLE bit_width scaled_run_len = do
  v <- takeBytesLe bit_width
  batch_bytes <- go 8 v
  (batch_bytes <>) <$> decodeBPLE bit_width (scaled_run_len - 1)
  where
    go :: Int -> Integer -> Get [Word32]
    go 0 data_bytes = pure []
    go rem_vals data_bytes = do
      let mask = (2 ^ bit_width) - 1
      -- Unsafe fromInteger justification:
      -- Max bit_width = 32 and masking any value with
      -- (2 ^ 32) - 1 is in unsigned 32-bit bound.
      let val = fromInteger $ data_bytes .&. mask
      rest <- go (rem_vals - 1) (data_bytes `shiftR` fromIntegral bit_width)
      pure $ val:rest

decodeRLE :: Word8 -> Word32 -> Get [Word32]
decodeRLE bit_width run_len =
  replicateM (fromIntegral run_len) $
    unsafe_bs_to_w32 . BS.unpack <$> getByteString (fromIntegral fixed_width)
  where
    fixed_width :: Word8
    fixed_width = (bit_width - 1) `div` 8

    -- TODO(yigitozkavci): We can do a safety check here. In
    -- case of overflow we get 0 as an answer.
    unsafe_bs_to_w32 :: [Word8] -> Word32
    unsafe_bs_to_w32 =
      foldr (\x -> ((fromIntegral x `shiftL` 8) .|.)) 0

decodeHybrid :: Word8 -> Get [Word32]
decodeHybrid bit_width = do
  len <- getWord32le
  run len
  where
    run :: Word32 -> Get [Word32]
    run 0 = pure []
    run rem = do
      header <- decodeVarint
      let encoding_ty = header .&. 0x01
      let run_len = header `shiftR` 1
      case encoding_ty of
        0x00 ->
          -- Unsafe fromInteger justification:
          -- run_len value being in range [1, 2^31-1]
          -- is guaranteed by the protocol.
          decodeRLE bit_width $ fromInteger run_len
        0x01 ->
          -- Unsafe fromInteger justification:
          -- run_len value being in range [1, 2^31-1]
          -- is guaranteed by the protocol.
          decodeBP_LE bit_width $ fromInteger run_len
        _ ->
          fail "Impossible happened! 0x01 .&. _ resulted in a value larger than 0x01"

decodeVarint :: Get Integer
decodeVarint = go cLeb128ByteLimit 0 0
  where
    go :: Int -> Integer -> Int -> Get Integer
    go 0 _ _ = fail $ "Could not find a LEB128-encoded value in " <> show cLeb128ByteLimit <> "bytes"
    go rem_limit acc sh = do
      byte <- getWord8
      let high = byte .&. 0x80
      let low = fromIntegral $ byte .&. 0x7F
      let res = (low `shiftL` sh) .|. acc
      if high == 0x80
        then go (rem_limit - 1) res (sh + 7)
        else pure res
