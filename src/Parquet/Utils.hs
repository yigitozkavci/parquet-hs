module Parquet.Utils where

import Control.Monad.Except
import qualified Data.Text as T

(<??>) :: MonadError b m => Maybe a -> b -> m a
(<??>) Nothing  err = throwError err
(<??>) (Just v) _   = pure v

infixl 4 <??>

(<???>) :: Monad m => Maybe a -> T.Text -> m a
(<???>) Nothing  err = fail (T.unpack err)
(<???>) (Just v) _   = pure v

infixl 4 <???>
