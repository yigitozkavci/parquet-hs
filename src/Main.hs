module Main where

import           Control.Monad.Except
import           Parquet.Reader
import           System.IO

main :: IO ()
main = openFile "test.parquet" ReadMode
   >>= runExceptT . goRead
   >>= print
