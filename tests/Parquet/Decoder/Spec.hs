module Parquet.Decoder.Spec (spec) where

import qualified Data.ByteString       as BS
import           Parquet.Decoder       (decodeRLE, decodeVarint)
import           Test.Hspec
import           Test.Hspec.QuickCheck
import           Test.QuickCheck

spec :: Spec
spec = describe "Decoder" $ do
  it "can decode from RLE/BP hybrid" $
    decodeRLE 3 (BS.pack [136, 198, 250]) `shouldBe` BS.pack [0, 1, 2, 3, 4, 5, 6, 7]

  it "can decode from varint (LEB128)" $
    decodeVarint (BS.pack [0xE5, 0x8E, 0x26]) `shouldBe` 624485
