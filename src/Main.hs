-- |
module Main
  ( main,
  )
where

------------------------------------------------------------------------------

import Conduit (runResourceT)
import Control.Monad.Logger
import Parquet.Prelude
import Parquet.Reader (readWholeParquetFile)

------------------------------------------------------------------------------
main :: IO ()
main = void . runT $ readWholeParquetFile "test.parquet"
  where
    runT = runStdoutLoggingT . runResourceT . runExceptT
