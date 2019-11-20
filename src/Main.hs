{-# LANGUAGE LambdaCase #-}

module Main where

import Conduit
import Control.Monad.Except
import Control.Monad.Logger
import Parquet.Stream.Reader (readContent, readMetadata)
import qualified Parquet.ThriftTypes as TT
import System.IO (IOMode(ReadMode), openFile)

main :: IO ()
main = do
  let file_name = "test.parquet"
  openFile file_name ReadMode >>= readMetadata >>= \case
    Left  err      -> fail $ "Could not read metadata: " <> show err
    Right metadata -> streamReadFile file_name metadata

streamReadFile :: FilePath -> TT.FileMetadata -> IO ()
streamReadFile file_name metadata =
  (print =<<)
    $          runStdoutLoggingT
    $          runResourceT
    $          runExceptT
    $          runConduit
    $          sourceFile file_name
    .|         readContent metadata
    `fuseBoth` pure ()
