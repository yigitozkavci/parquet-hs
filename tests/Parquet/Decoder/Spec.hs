module Parquet.Decoder.Spec (spec) where

import           Data.Binary.Get       (getWord8, lookAhead, runGetOrFail)
import           Data.Binary.Put
import qualified Data.ByteString       as BS
import qualified Data.ByteString.Lazy  as LBS
import           Parquet.Decoder
import           Test.Hspec
import           Test.Hspec.QuickCheck
import           Test.QuickCheck

spec :: Spec
spec = describe "Decoder" $ do
  it "can decode from little endian bit packing" $
    runGetOrFail (decodeBPLE 3 1) (LBS.pack [136, 198, 250]) `shouldBe` Right (LBS.empty, 3, [0, 1, 2, 3, 4, 5, 6, 7])

  it "can decode from big endian bit packing" $
    runGetOrFail (decodeBPBE 3) (runPut (encodeVarint 3) <> LBS.pack [5, 57, 119]) `shouldBe` Right (LBS.empty, 4, [0, 1, 2, 3, 4, 5, 6, 7])

  it "can takeBytesLe" $
    runGetOrFail (takeBytesLe 3) (LBS.pack [136, 198, 250]) `shouldBe` Right (LBS.empty, 3, 16434824)

  it "can takeBytesLe with leftovers" $
    runGetOrFail (takeBytesLe 3) (LBS.pack [136, 198, 250, 1, 2]) `shouldBe` Right (LBS.pack [1, 2], 3, 16434824)

  it "can encode varint (LEB128) exactly" $
    runPut (encodeVarint 624485) `shouldBe` LBS.pack [0xE5, 0x8E, 0x26]

  it "can decode from varint (LEB128) exactly" $
    runGetOrFail decodeVarint (LBS.pack [0xE5, 0x8E, 0x26]) `shouldBe` Right (LBS.empty, 3, 624485)

  it "can decode from varint (LEB128) with leftovers" $
    runGetOrFail decodeVarint (LBS.pack [0xE5, 0x8E, 0x26, 1, 2]) `shouldBe` Right (LBS.pack [1, 2], 3, 624485)

  it "can decode from varint (LEB128) with leftovers and lookahead" $
    runGetOrFail decodeVarint (LBS.pack [0xE5, 0x8E, 0x26, 1, 2]) `shouldBe` Right (LBS.pack [1, 2], 3, 624485)
