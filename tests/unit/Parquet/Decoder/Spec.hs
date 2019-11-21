module Parquet.Decoder.Spec
  ( spec
  )
where

import Data.Binary.Get (getWord8, lookAhead, runGetOrFail)
import Data.Binary.Put
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as LBS
import Parquet.Decoder
import Test.Hspec
import Test.Hspec.QuickCheck
import Test.QuickCheck

spec :: Spec
spec = describe "Decoder" $ do
  it "can decode from little endian bit packing"
    $ runGetOrFail (decodeBPLE (BitWidth 3) 1) (LBS.pack [136, 198, 250])
    `shouldBe` Right (LBS.empty, 3, [0, 1, 2, 3, 4, 5, 6, 7])

  it "can decode from little endian bit packing with 0 padding"
    $
    -- There are 5 values: [0, 5)
    -- Bit packing is 3
    -- Scaled run len is: 1 (and run len is 8) because we keep 5 values
    --
    -- Result type is (Either Error (Unconsumed bytes, consumed byte len, result))
    -- In this case we decode 5 values but due to parquet only bit-packs
    -- a multiple of 8 values at once, we still have to consume them.
    -- Docs:
    -- https://github.com/apache/parquet-format/blob/master/Encodings.md#run-length-encoding--bit-packing-hybrid-rle--3
               runGetOrFail
                 (decodeBPLE (BitWidth 3) 1)
                 (LBS.pack [0x88, 0x46, 0x00, 0x00])
    `shouldBe` Right (LBS.pack [0], 3, [0, 1, 2, 3, 4, 0, 0, 0])

  it "can decode from big endian bit packing"
    $          runGetOrFail
                 (decodeBPBE (BitWidth 3))
                 (runPut (encodeVarint 3) <> LBS.pack [5, 57, 119])
    `shouldBe` Right (LBS.empty, 4, [0, 1, 2, 3, 4, 5, 6, 7])

  it "can do run length encoding (RLE)"
    $ runGetOrFail (decodeRLE (BitWidth 3) 4) (LBS.pack [1, 2, 3, 4, 5])
    `shouldBe` Right (LBS.pack [2, 3, 4, 5], 1, [1, 1, 1, 1])

  it "can takeBytesLe"
    $          runGetOrFail (takeBytesLe 3) (LBS.pack [136, 198, 250])
    `shouldBe` Right (LBS.empty, 3, 16434824)

  it "can takeBytesLe with leftovers"
    $          runGetOrFail (takeBytesLe 3) (LBS.pack [136, 198, 250, 1, 2])
    `shouldBe` Right (LBS.pack [1, 2], 3, 16434824)

  it "can encode varint (LEB128) exactly"
    $          runPut (encodeVarint 624485)
    `shouldBe` LBS.pack [0xE5, 0x8E, 0x26]

  it "can decode from varint (LEB128) exactly"
    $          runGetOrFail decodeVarint (LBS.pack [0xE5, 0x8E, 0x26])
    `shouldBe` Right (LBS.empty, 3, 624485)

  it "can decode from varint (LEB128) with leftovers"
    $          runGetOrFail decodeVarint (LBS.pack [0xE5, 0x8E, 0x26, 1, 2])
    `shouldBe` Right (LBS.pack [1, 2], 3, 624485)

  it "can decode from varint (LEB128) with leftovers and lookahead"
    $          runGetOrFail decodeVarint (LBS.pack [0xE5, 0x8E, 0x26, 1, 2])
    `shouldBe` Right (LBS.pack [1, 2], 3, 624485)
