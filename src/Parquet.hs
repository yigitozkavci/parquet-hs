module Parquet
  ( main,
  )
where

------------------------------------------------------------------------------

import qualified Conduit as C
import Control.Monad.Logger
import Parquet.Prelude
import Parquet.Reader (readWholeParquetFile)

------------------------------------------------------------------------------
main :: IO ()
main =
  let fp = "test.parquet"
   in void . runStdoutLoggingT . C.runResourceT $
        runExceptT (readWholeParquetFile fp)
