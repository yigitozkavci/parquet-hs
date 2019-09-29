{-# LANGUAGE FlexibleContexts     #-}

module Parquet.PREnv where

import qualified Data.Map as M
import qualified Data.Text as T
import qualified Parquet.ThriftTypes        as TT

newtype PREnv = PREnv { _prEnvSchema :: M.Map T.Text TT.SchemaElement }

