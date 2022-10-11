module Main (main) where

import qualified Parquet.Decoder.Spec
import Parquet.Prelude
import Test.Hspec

main :: IO ()
main =
  hspec Parquet.Decoder.Spec.spec
