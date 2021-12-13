module Main
  ( main,
  )
where

import Conduit (runResourceT)
import Control.Exception (bracket_)
import Control.Monad.Except (runExceptT)
import Control.Monad.IO.Class (liftIO)
import Control.Monad.Logger
import qualified Data.Aeson as JSON
import qualified Data.ByteString.Lazy as BS
import qualified Data.ByteString.Lazy as LByteString (ByteString, toStrict)
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import qualified Data.Text.IO as TextIO (putStrLn)
import qualified Data.Text.Lazy as LText (Text)
import qualified Data.Text.Lazy.IO as LTextIO (putStrLn)
import Parquet.Reader (readWholeParquetFile)
import System.Environment (setEnv, unsetEnv)
import System.FilePath ((</>))
import System.Process
import Test.Hspec

testPath :: String
testPath = "tests" </> "integration"

testDataPath :: String
testDataPath = testPath </> "testdata"

intermediateDir :: String
intermediateDir = "nested.parquet"

encoderScriptPath :: String
encoderScriptPath = "gen_parquet.py"

outParquetFilePath :: String
outParquetFilePath = testPath </> "test.parquet"

pysparkPythonEnvName :: String
pysparkPythonEnvName = "PYSPARK_PYTHON"

testParquetFormat :: String -> (String -> IO ()) -> IO ()
testParquetFormat inputFile performTest =
  bracket_
    (setEnv pysparkPythonEnvName "/usr/bin/python3")
    (unsetEnv pysparkPythonEnvName)
    $ do
      callProcess
        "python3"
        [ testPath </> encoderScriptPath,
          testDataPath </> "input1.json",
          testPath </> intermediateDir
        ]
      callCommand $
        "cp "
          <> testPath
          </> intermediateDir
          </> "*.parquet "
          <> outParquetFilePath
      performTest $ outParquetFilePath
      callProcess "rm" ["-rf", testPath </> intermediateDir]

-- callProcess "rm" ["-f", outParquetFilePath]

lazyByteStringToText :: LByteString.ByteString -> T.Text
lazyByteStringToText = T.decodeUtf8 . LByteString.toStrict

lazyByteStringToString :: LByteString.ByteString -> String
lazyByteStringToString = T.unpack . lazyByteStringToText

putLazyByteStringLn :: LByteString.ByteString -> IO ()
putLazyByteStringLn = TextIO.putStrLn . lazyByteStringToText

putLazyTextLn :: LText.Text -> IO ()
putLazyTextLn = LTextIO.putStrLn

main :: IO ()
main = hspec $
  describe "Reader" $ do
    it "can read columns" $ do
      testParquetFormat "input1.json" $ \parqFile -> do
        result <-
          runResourceT
            (runStdoutLoggingT (runExceptT (readWholeParquetFile parqFile)))
        case result of
          Left err -> fail $ show err
          Right v -> liftIO $ BS.putStrLn $ JSON.encode v
        pure ()
