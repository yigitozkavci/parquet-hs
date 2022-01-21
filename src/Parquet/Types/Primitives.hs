module Parquet.Types.Primitives
  ( -- * Type definitions
    AesGcmV1 (..),
    AesGcmCtrV1 (..),
    EncryptionWithColumnKey (..),
    EncryptionWithFooterKey (..),
    KeyValue (..),
    Statistics (..),
    TypeDefinedOrder (..),

    -- * Primitive wrappers
    BsonType (..),
    DateType (..),
    DecimalType (..),
    EnumType (..),
    IntType (..),
    JsonType (..),
    ListType (..),
    MapType (..),
    NullType (..),
    StringType (..),
    TimestampType (..),
    TimeType (..),
    UUIDType (..),

    -- * Internal type definitions
    TimeUnit (..),
    MicroSeconds (..),
    MilliSeconds (..),
    NanoSeconds (..),
  )
where

------------------------------------------------------------------------------

import Parquet.Prelude
import Pinch

------------------------------------------------------------------------------
data AesGcmV1 = AesGcmV1
  { _AesGcmV1_aad_prefix :: Field 1 (Maybe ByteString),
    _AesGcmV1_aad_file_unique :: Field 2 (Maybe ByteString),
    _AesGcmV1_supply_aad_prefix :: Field 3 (Maybe Bool)
  }
  deriving (Show, Eq, Generic, Pinchable, Binary)

------------------------------------------------------------------------------
data AesGcmCtrV1 = AesGcmCtrV1
  { _AesGcmCtrV1_aad_prefix :: Field 1 (Maybe ByteString),
    _AesGcmCtrV1_aad_file_unique :: Field 2 (Maybe ByteString),
    _AesGcmCtrV1_supply_aad_prefix :: Field 3 (Maybe Bool)
  }
  deriving (Show, Eq, Generic, Pinchable, Binary)

------------------------------------------------------------------------------
data EncryptionWithColumnKey = EncryptionWithColumnKey
  { _EncryptionWithColumnKey_path_in_schema :: Field 1 [Text],
    _EncryptionWithColumnKey_key_metadata :: Field 2 (Maybe ByteString)
  }
  deriving (Show, Eq, Generic, Pinchable, Binary)

------------------------------------------------------------------------------
data EncryptionWithFooterKey = EncryptionWithFooterKey
  deriving (Show, Eq, Generic, Binary)

instance Pinchable EncryptionWithFooterKey where
  type Tag EncryptionWithFooterKey = TStruct
  pinch _ = struct []
  unpinch _ = pure EncryptionWithFooterKey

------------------------------------------------------------------------------
data KeyValue = KeyValue
  { _KeyValue_key :: Field 1 Text,
    _KeyValue_value :: Field 2 (Maybe Text)
  }
  deriving (Show, Eq, Generic, Pinchable, Binary)

------------------------------------------------------------------------------
data Statistics = Statistics
  { _Statistics_max :: Field 1 (Maybe ByteString),
    _Statistics_min :: Field 2 (Maybe ByteString),
    _Statistics_null_count :: Field 3 (Maybe Int64),
    _Statistics_distinct_count :: Field 4 (Maybe Int64),
    _Statistics_max_value :: Field 5 (Maybe ByteString),
    _Statistics_min_value :: Field 6 (Maybe ByteString)
  }
  deriving (Show, Eq, Generic, Pinchable, Binary)

------------------------------------------------------------------------------
data TypeDefinedOrder = TypeDefinedOrder
  deriving (Show, Eq, Generic, Binary)

instance Pinchable TypeDefinedOrder where
  type Tag TypeDefinedOrder = TStruct
  pinch _ = struct []
  unpinch _ = pure TypeDefinedOrder

------------------------------------------------------------------------------
data BsonType = BsonType
  deriving (Show, Eq, Generic, Binary)

instance Pinchable BsonType where
  type Tag BsonType = TStruct
  pinch _ = struct []
  unpinch _ = pure BsonType

------------------------------------------------------------------------------
data DateType = DateType
  deriving (Show, Eq, Generic, Binary)

instance Pinchable DateType where
  type Tag DateType = TStruct
  pinch _ = struct []
  unpinch _ = pure DateType

------------------------------------------------------------------------------
data DecimalType = DecimalType
  { _DecimalType_scale :: Field 1 Int32,
    _DecimalType_precision :: Field 2 Int32
  }
  deriving (Show, Eq, Generic, Pinchable, Binary)

------------------------------------------------------------------------------
data EnumType = EnumType
  deriving (Show, Eq, Generic, Binary)

instance Pinchable EnumType where
  type Tag EnumType = TStruct
  pinch _ = struct []
  unpinch _ = pure EnumType

------------------------------------------------------------------------------
data IntType = IntType
  { _IntType_bitWidth :: Field 1 Int8,
    _IntType_isSigned :: Field 2 Bool
  }
  deriving (Show, Eq, Generic, Pinchable, Binary)

------------------------------------------------------------------------------
data JsonType = JsonType
  deriving (Show, Eq, Generic, Binary)

instance Pinchable JsonType where
  type Tag JsonType = TStruct
  pinch _ = struct []
  unpinch _ = pure JsonType

------------------------------------------------------------------------------
data ListType = ListType
  deriving (Show, Eq, Generic, Binary)

instance Pinchable ListType where
  type Tag ListType = TStruct
  pinch _ = struct []
  unpinch _ = pure ListType

------------------------------------------------------------------------------
data MapType = MapType
  deriving (Show, Eq, Generic, Binary)

instance Pinchable MapType where
  type Tag MapType = TStruct
  pinch _ = struct []
  unpinch _ = pure MapType

------------------------------------------------------------------------------
data MicroSeconds = MicroSeconds
  deriving (Show, Eq, Generic, Binary)

instance Pinchable MicroSeconds where
  type Tag MicroSeconds = TStruct
  pinch _ = struct []
  unpinch _ = pure MicroSeconds

------------------------------------------------------------------------------
data MilliSeconds = MilliSeconds
  deriving (Show, Eq, Generic, Binary)

instance Pinchable MilliSeconds where
  type Tag MilliSeconds = TStruct
  pinch _ = struct []
  unpinch _ = pure MilliSeconds

------------------------------------------------------------------------------
data NanoSeconds = NanoSeconds
  deriving (Show, Eq, Generic, Binary)

instance Pinchable NanoSeconds where
  type Tag NanoSeconds = TStruct
  pinch _ = struct []
  unpinch _ = pure NanoSeconds

------------------------------------------------------------------------------
data NullType = NullType
  deriving (Show, Eq, Generic, Binary)

instance Pinchable NullType where
  type Tag NullType = TStruct
  pinch _ = struct []
  unpinch _ = pure NullType

------------------------------------------------------------------------------
data StringType = StringType
  deriving (Show, Eq, Generic, Binary)

instance Pinchable StringType where
  type Tag StringType = TStruct
  pinch _ = struct []
  unpinch _ = pure StringType

------------------------------------------------------------------------------
data TimestampType = TimestampType
  { _TimestampType_isAdjustedToUTC :: Field 1 Bool,
    _TimestampType_unit :: Field 2 TimeUnit
  }
  deriving (Show, Eq, Generic, Pinchable, Binary)

------------------------------------------------------------------------------
data TimeType = TimeType
  { _TimeType_isAdjustedToUTC :: Field 1 Bool,
    _TimeType_unit :: Field 2 TimeUnit
  }
  deriving (Show, Eq, Generic, Pinchable, Binary)

------------------------------------------------------------------------------
data TimeUnit
  = TimeUnitMILLIS (Field 1 MilliSeconds)
  | TimeUnitMICROS (Field 2 MicroSeconds)
  | TimeUnitNANOS (Field 3 NanoSeconds)
  deriving (Show, Eq, Generic, Binary)

instance Pinchable TimeUnit

------------------------------------------------------------------------------
data UUIDType = UUIDType
  deriving (Show, Eq, Generic, Binary)

instance Pinchable UUIDType where
  type Tag UUIDType = TStruct
  pinch _ = struct []
  unpinch _ = pure UUIDType
