-- |
module Parquet.Monad where

------------------------------------------------------------------------------

import Conduit (MonadResource, MonadThrow)
import Control.Monad.Except (MonadError)
import Control.Monad.Logger (MonadLogger)
import Parquet.Prelude

------------------------------------------------------------------------------

type PR m =
  (MonadResource m, MonadLogger m, MonadThrow m, MonadError Text m)
