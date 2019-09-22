module Main where

import           Parquet.Reader
import           System.IO

main :: IO ()
main =
  goRead =<< openFile "test.parquet" ReadMode
