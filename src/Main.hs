module Main where

import Parquet.Reader (readWholeParquetFile)
import Control.Monad.Except
import qualified Conduit as C

main :: IO ()
main = do
  let fp = "test.parquet"
  print =<< C.runResourceT (runExceptT (readWholeParquetFile fp))
