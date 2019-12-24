{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE TemplateHaskell #-}

module Parquet.ParquetObject where

import Control.Lens
import Data.Binary (Binary(..))
import Codec.Serialise (Serialise)
import GHC.Generics (Generic)
import qualified Data.ByteString as BS
import qualified Data.HashMap.Strict as HM
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import Data.Int (Int64)
import qualified Data.Aeson as JSON

newtype ParquetObject = MkParquetObject (HM.HashMap T.Text ParquetValue)
  deriving (Eq, Show, Generic, Serialise)

instance Semigroup ParquetObject where
  MkParquetObject hm1 <> MkParquetObject hm2 = MkParquetObject (hm1 <> hm2)

instance Monoid ParquetObject where
  mempty = MkParquetObject mempty

instance Binary ParquetObject where
  put (MkParquetObject hm) = put (HM.toList hm)
  get = MkParquetObject . HM.fromList <$> get

instance JSON.ToJSON ParquetObject where
  toJSON (MkParquetObject obj) = JSON.toJSON obj

newtype ParquetList = MkParquetList [ParquetValue]
  deriving (Eq, Show, Generic, Serialise)

instance Semigroup ParquetList where
  MkParquetList l1 <> MkParquetList l2 = MkParquetList (l1 <> l2)

instance Monoid ParquetList where
  mempty = MkParquetList mempty

instance Binary ParquetList where
  put (MkParquetList l) = put l
  get = MkParquetList <$> get

instance JSON.ToJSON ParquetList where
  toJSON (MkParquetList l) = JSON.toJSON l

data ParquetValue =
    ParquetObject !ParquetObject
  | ParquetList !ParquetList
  | ParquetInt !Int64
  | ParquetString !BS.ByteString
  | ParquetNull
  | EmptyValue
  deriving (Eq, Show, Generic, Binary, Serialise)

instance JSON.ToJSON ParquetValue where
  toJSON = \case
    ParquetObject obj -> JSON.toJSON obj
    ParquetList   l   -> JSON.toJSON l
    ParquetInt    i64 -> JSON.Number (fromIntegral i64)
    ParquetString bs  -> case T.decodeUtf8' bs of
      Right t -> JSON.String t
      Left  _ -> JSON.String "<non-utf8-string>"
    ParquetNull -> JSON.Null
    EmptyValue  -> JSON.Null

makeLenses ''ParquetObject
makePrisms ''ParquetObject

makeLenses ''ParquetValue
makePrisms ''ParquetValue
