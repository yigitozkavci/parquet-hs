{-# LANGUAGE ConstraintKinds  #-}
{-# LANGUAGE FlexibleContexts #-}

module Parquet.Monad where

import qualified Conduit              as C
import           Control.Monad.Except
import           Control.Monad.Logger
import qualified Data.Text            as T

type PR m = (C.MonadResource m, MonadLogger m, C.MonadThrow m, MonadError T.Text m)
