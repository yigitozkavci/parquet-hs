module Main where

import qualified Data.ByteString.Char8 as BS
import           Parquet.Reader
import           Parquet.ThriftTypes
import           System.IO

main :: IO ()
main =
  goRead =<< openFile "test.parquet" ReadMode
