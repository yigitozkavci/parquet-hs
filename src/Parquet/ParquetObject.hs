{-# LANGUAGE DeriveGeneric #-}
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
import Data.Int (Int64)

newtype ParquetObject = MkParquetObject (HM.HashMap T.Text ParquetValue)
  deriving (Eq, Show, Generic, Serialise)

instance Semigroup ParquetObject where
  MkParquetObject hm1 <> MkParquetObject hm2 = MkParquetObject (hm1 <> hm2)

instance Monoid ParquetObject where
  mempty = MkParquetObject mempty

instance Binary ParquetObject where
  put (MkParquetObject hm) = put (HM.toList hm)
  get = MkParquetObject . HM.fromList <$> get

data ParquetValue =
    ParquetObject !ParquetObject
  | ParquetInt !Int64
  | ParquetString !BS.ByteString
  | ParquetNull
  deriving (Eq, Show, Generic, Binary, Serialise)

makeLenses ''ParquetObject
makePrisms ''ParquetObject

makeLenses ''ParquetValue
makePrisms ''ParquetValue
