module Main
  ( main
  )
where

import Test.Hspec
import System.Process
import System.Environment (setEnv, unsetEnv)
import System.FilePath ((</>))
import Control.Exception (bracket_)

import Parquet.Reader (readWholeParquetFile)

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
          [ testPath </> encoderScriptPath
          , testDataPath </> "input1.json"
          , testPath </> intermediateDir
          ]
        callCommand
          $   "cp "
          <>  testPath
          </> intermediateDir
          </> "*.parquet "
          <>  outParquetFilePath
        performTest $ outParquetFilePath
        callProcess "rm" ["-rf", testPath </> intermediateDir]
        -- callProcess "rm" ["-f", outParquetFilePath]

main :: IO ()
main = hspec $ describe "Reader" $ do
  it "can read columns" $ do
    testParquetFormat "input1.json" $ \parqFile -> do
      _ <- readWholeParquetFile parqFile
      pure ()
