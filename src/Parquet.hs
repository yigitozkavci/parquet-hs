module Parquet where

import qualified Conduit as C
import Control.Monad.Except
import Control.Monad.Logger
import Parquet.Prelude
import Parquet.Reader (readWholeParquetFile)

main :: IO ()
main = do
  let fp = "test.parquet"
  void $
    runStdoutLoggingT $
      C.runResourceT $
        runExceptT $
          readWholeParquetFile
            fp
