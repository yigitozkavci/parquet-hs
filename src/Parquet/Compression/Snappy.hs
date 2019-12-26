{-# LANGUAGE ScopedTypeVariables #-}

module Parquet.Compression.Snappy
  ( compress
  , decompress
  )
where

import qualified Data.Text as T
import Control.Exception
  (catch, IOException, evaluate, SomeException, Handler(..), ErrorCall)
import qualified Codec.Compression.Snappy as Snappy

decompress bs = (Right <$> evaluate (Snappy.decompress bs))
  `catch` \(e :: SomeException) -> pure (Left $ T.pack $ show e)

compress = Snappy.compress
