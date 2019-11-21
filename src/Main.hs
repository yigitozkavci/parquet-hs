module Main where

import Parquet.Reader (readWholeParquetFile)

main :: IO ()
main = do
  let fp = "test.parquet"
  print =<< readWholeParquetFile fp
