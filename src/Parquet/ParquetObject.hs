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
newtype ParquetObject = ParquetObject (HashMap Text ParquetValue)
  deriving (Eq, Show, Generic, Serialise)

instance Semigroup ParquetObject where
  ParquetObject hm1 <> ParquetObject hm2 = ParquetObject (hm1 <> hm2)

instance Monoid ParquetObject where
  mempty = ParquetObject mempty

instance Binary ParquetObject where
  put (ParquetObject hm) = put (toList hm)
  get = ParquetObject . fromList <$> get

instance ToJSON ParquetObject where
  toJSON (ParquetObject obj) = toJSON obj

------------------------------------------------------------------------------

-- |
newtype ParquetList = ParquetList [ParquetValue]
  deriving (Eq, Show, Generic, Serialise)

instance Semigroup ParquetList where
  ParquetList l1 <> ParquetList l2 = ParquetList (l1 <> l2)

instance Monoid ParquetList where
  mempty = ParquetList mempty

instance Binary ParquetList where
  put (ParquetList l) = put l
  get = ParquetList <$> get

instance ToJSON ParquetList where
  toJSON (ParquetList l) = toJSON l

------------------------------------------------------------------------------

-- |
data ParquetValue
  = ParquetValue_Object !ParquetObject
  | ParquetValue_List !ParquetList
  | ParquetValue_Int !Int64
  | ParquetValue_String !ByteString
  | ParquetValue_Bool !Bool
  | ParquetValue_Null
  | ParquetValue_Empty
  deriving (Eq, Show, Generic, Binary, Serialise)

instance FromJSON ParquetValue where
  parseJSON = \case
    A.Object obj -> do
      ParquetValue_Object . ParquetObject <$> traverse parseJSON obj
    A.Array vec -> do
      ParquetValue_List . ParquetList . toList <$> traverse parseJSON vec
    A.Number sci ->
      pure $ ParquetValue_Int $ fromInteger $ numerator $ toRational sci
    A.String s ->
      pure $ ParquetValue_String $ encodeUtf8 s
    A.Bool b -> pure $ ParquetValue_Bool b
    A.Null -> pure ParquetValue_Null

instance ToJSON ParquetValue where
  toJSON = \case
    ParquetValue_Object obj -> toJSON obj
    ParquetValue_List l -> toJSON l
    ParquetValue_Int i64 -> A.Number (fromIntegral i64)
    ParquetValue_String bs -> case decodeUtf8' bs of
      Right t -> A.String t
      Left _ -> A.String "<non-utf8-string>"
    ParquetValue_Bool b -> A.Bool b
    ParquetValue_Null -> A.Null
    ParquetValue_Empty -> A.Null
