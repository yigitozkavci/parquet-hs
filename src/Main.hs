module Main where

import           Parquet.Reader
import           System.IO
import Control.Monad.Except

main :: IO ()
main = openFile "test.parquet" ReadMode
   >>= runExceptT . goRead
   >>= print
