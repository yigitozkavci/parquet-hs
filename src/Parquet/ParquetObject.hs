-- |
module Parquet.ParquetObject where

------------------------------------------------------------------------------

import Codec.Serialise (Serialise)
import Data.Aeson (FromJSON (..), ToJSON (..))
import qualified Data.Aeson as A
import Data.Binary (Binary (..))
import Parquet.Prelude

------------------------------------------------------------------------------

-- |
newtype ParquetObject = MkParquetObject (HashMap Text ParquetValue)
  deriving (Eq, Show, Generic, Serialise)

instance Semigroup ParquetObject where
  MkParquetObject hm1 <> MkParquetObject hm2 = MkParquetObject (hm1 <> hm2)

instance Monoid ParquetObject where
  mempty = MkParquetObject mempty

instance Binary ParquetObject where
  put (MkParquetObject hm) = put (toList hm)
  get = MkParquetObject . fromList <$> get

instance ToJSON ParquetObject where
  toJSON (MkParquetObject obj) = toJSON obj

------------------------------------------------------------------------------

-- |
newtype ParquetList = MkParquetList [ParquetValue]
  deriving (Eq, Show, Generic, Serialise)

instance Semigroup ParquetList where
  MkParquetList l1 <> MkParquetList l2 = MkParquetList (l1 <> l2)

instance Monoid ParquetList where
  mempty = MkParquetList mempty

instance Binary ParquetList where
  put (MkParquetList l) = put l
  get = MkParquetList <$> get

instance ToJSON ParquetList where
  toJSON (MkParquetList l) = toJSON l

------------------------------------------------------------------------------

-- |
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
    A.Object obj -> do
      ParquetObject . MkParquetObject <$> traverse parseJSON obj
    A.Array vec -> do
      ParquetList . MkParquetList . toList <$> traverse parseJSON vec
    A.Number sci ->
      pure $ ParquetInt $ fromInteger $ numerator $ toRational sci
    A.String s ->
      pure $ ParquetString $ encodeUtf8 s
    A.Bool b -> pure $ ParquetBool b
    A.Null -> pure ParquetNull

instance ToJSON ParquetValue where
  toJSON = \case
    ParquetObject obj -> toJSON obj
    ParquetList l -> toJSON l
    ParquetInt i64 -> A.Number (fromIntegral i64)
    ParquetString bs -> case decodeUtf8' bs of
      Right t -> A.String t
      Left _ -> A.String "<non-utf8-string>"
    ParquetBool b -> A.Bool b
    ParquetNull -> A.Null
    EmptyValue -> A.Null
