{-# LANGUAGE TemplateHaskell #-}

-- |
module Parquet.ParquetObject
  ( -- * Type definitions
    ParquetValue (..),
    ParquetObject (..),
    ParquetList (..),
  )
where

------------------------------------------------------------------------------

import Lens.Micro.Platform (makeLenses)
import qualified Data.Aeson as JSON
import Data.Binary (Binary (get, put))
import Parquet.Prelude hiding (get, put)

------------------------------------------------------------------------------
newtype ParquetObject = MkParquetObject (HashMap Text ParquetValue)
  deriving (Eq, Show, Generic)
  deriving newtype (Serialise, Semigroup, Monoid)

instance Binary ParquetObject where
  put (MkParquetObject hm) = put (toList hm)
  get = MkParquetObject . fromList <$> get

instance ToJSON ParquetObject where
  toJSON (MkParquetObject obj) = toJSON obj

------------------------------------------------------------------------------
newtype ParquetList = MkParquetList [ParquetValue]
  deriving (Eq, Show, Generic)
  deriving newtype (Serialise, Semigroup, Monoid)

instance Binary ParquetList where
  put (MkParquetList l) = put l
  get = MkParquetList <$> get

instance ToJSON ParquetList where
  toJSON (MkParquetList l) = toJSON l

------------------------------------------------------------------------------
data ParquetValue
  = ParquetObject !ParquetObject
  | ParquetList !ParquetList
  | ParquetInt !Int64
  | ParquetString !ByteString
  | ParquetBool !Bool
  | ParquetNull
  | EmptyValue
  deriving (Eq, Show, Generic, Binary, Serialise)

instance FromJSON ParquetValue where
  parseJSON = \case
    JSON.Object obj -> do
      ParquetObject . MkParquetObject <$> traverse parseJSON obj
    JSON.Array vec -> do
      ParquetList . MkParquetList . toList <$> traverse parseJSON vec
    JSON.Number sci ->
      pure $ ParquetInt $ fromInteger $ numerator $ toRational sci
    JSON.String s ->
      pure $ ParquetString $ encodeUtf8 s
    JSON.Bool b -> pure $ ParquetBool b
    JSON.Null -> pure ParquetNull

instance ToJSON ParquetValue where
  toJSON = \case
    ParquetObject obj -> toJSON obj
    ParquetList l -> toJSON l
    ParquetInt i64 -> JSON.Number (fromIntegral i64)
    ParquetString bs -> case decodeUtf8' bs of
      Right t -> JSON.String t
      Left _ -> JSON.String "<non-utf8-string>"
    ParquetBool b -> JSON.Bool b
    ParquetNull -> JSON.Null
    EmptyValue -> JSON.Null

------------------------------------------------------------------------------
makeLenses ''ParquetObject
makeLenses ''ParquetValue

