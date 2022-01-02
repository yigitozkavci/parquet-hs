-- |
module Parquet.Utils
  ( -- * Helper functions
    failOnExcept,
    failOnMay,

    -- * Operators
    (<??>),
  )
where

------------------------------------------------------------------------------

import Control.Monad.Except
import Data.Text (unpack)
import Parquet.Prelude

------------------------------------------------------------------------------

(<??>) :: MonadError b m => Maybe a -> b -> m a
(<??>) Nothing err = throwError err
(<??>) (Just v) _ = pure v

infixl 4 <??>

------------------------------------------------------------------------------

-- |
failOnExcept ::
  ( Monad m,
    MonadFail m
  ) =>
  ExceptT Text m a ->
  m a
failOnExcept =
  runExceptT >=> \case
    Left err -> fail (unpack err)
    Right v -> pure v

------------------------------------------------------------------------------
-- |
failOnMay ::
  ( Monad m,
    MonadFail m
  ) =>
  Maybe a ->
  String ->
  m a
failOnMay Nothing s = fail s
failOnMay (Just a) _ = pure a
