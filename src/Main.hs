module Main where

import Parquet.Reader (readWholeParquetFile)
import Control.Monad.Except
import qualified Conduit as C
import Control.Monad.Logger

main :: IO ()
main = do
  let fp = "test.parquet"
  void $ runNoLoggingT $ C.runResourceT $ runExceptT $ readWholeParquetFile fp
