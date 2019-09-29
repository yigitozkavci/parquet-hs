{-# LANGUAGE ConstraintKinds  #-}
{-# LANGUAGE FlexibleContexts #-}

module Parquet.Monad where

import qualified Conduit              as C
import           Control.Monad.Except
import           Control.Monad.Logger
import           Control.Monad.Reader
import qualified Data.Text            as T
import           Parquet.PREnv        (PREnv)

type PR m = (MonadLogger m, C.MonadThrow m, MonadReader PREnv m, MonadError T.Text m)
