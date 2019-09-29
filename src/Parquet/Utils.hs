module Parquet.Utils where

import Control.Monad.Except

(<??>) :: MonadError b m => Maybe a -> b -> m a
(<??>) Nothing err = throwError err
(<??>) (Just v) _ = pure v

infixl 4 <??>
