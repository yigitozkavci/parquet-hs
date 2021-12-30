{-# LANGUAGE LambdaCase #-}

module Parquet.Utils where

import Control.Monad.Except
import qualified Data.Text as T

(<??>) :: MonadError b m => Maybe a -> b -> m a
(<??>) Nothing err = throwError err
(<??>) (Just v) _ = pure v

infixl 4 <??>

failOnMay ::
  ( Monad m,
    MonadFail m
  ) =>
  Maybe a ->
  String ->
  m a
failOnMay Nothing s = fail s
failOnMay (Just a) _ = pure a

failOnExcept ::
  ( Monad m,
    MonadFail m
  ) =>
  ExceptT T.Text m a ->
  m a
failOnExcept =
  runExceptT >=> \case
    Left err -> fail (T.unpack err)
    Right v -> pure v
