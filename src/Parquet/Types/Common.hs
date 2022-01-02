-- |
module Parquet.Types.Common
  ( -- * Type definitions
    BsonType,
    EnumType,
    DateType,
    DecimalType,
    IntType,
    JsonType,
    ListType,
    MapType,
    NullType,
    StringType,
    TimestampType,
    TimeType,
    Type (..),
    UUIDType,
  )
where

------------------------------------------------------------------------------

import Parquet.Prelude
import Pinch

------------------------------------------------------------------------------

-- |
data Type
  = BOOLEAN (Enumeration 0)
  | INT32 (Enumeration 1)
  | INT64 (Enumeration 2)
  | INT96 (Enumeration 3)
  | FLOAT (Enumeration 4)
  | DOUBLE (Enumeration 5)
  | BYTE_ARRAY (Enumeration 6)
  | FIXED_LEN_BYTE_ARRAY (Enumeration 7)
  deriving (Show, Eq, Generic, Pinchable, Binary)

------------------------------------------------------------------------------

-- |
data StringType = StringType
  deriving (Show, Eq, Generic, Binary)

instance Pinchable StringType where
  type Tag StringType = TStruct
  pinch _ = struct []
  unpinch _ = pure StringType

------------------------------------------------------------------------------

-- |
data UUIDType = UUIDType
  deriving (Show, Eq, Generic, Binary)

instance Pinchable UUIDType where
  type Tag UUIDType = TStruct
  pinch _ = struct []
  unpinch _ = pure UUIDType

------------------------------------------------------------------------------

-- |
data MapType = MapType
  deriving (Show, Eq, Generic, Binary)

instance Pinchable MapType where
  type Tag MapType = TStruct
  pinch _ = struct []
  unpinch _ = pure MapType

------------------------------------------------------------------------------

-- |
data ListType = ListType
  deriving (Show, Eq, Generic, Binary)

instance Pinchable ListType where
  type Tag ListType = TStruct
  pinch _ = struct []
  unpinch _ = pure ListType

------------------------------------------------------------------------------

-- |
data EnumType = EnumType
  deriving (Show, Eq, Generic, Binary)

instance Pinchable EnumType where
  type Tag EnumType = TStruct
  pinch _ = struct []
  unpinch _ = pure EnumType

------------------------------------------------------------------------------

-- |
data DateType = DateType
  deriving (Show, Eq, Generic, Binary)

instance Pinchable DateType where
  type Tag DateType = TStruct
  pinch _ = struct []
  unpinch _ = pure DateType

------------------------------------------------------------------------------

-- |
data DecimalType = DecimalType
  { _DecimalType_scale :: Field 1 Int32,
    _DecimalType_precision :: Field 2 Int32
  }
  deriving (Show, Eq, Generic, Pinchable, Binary)

------------------------------------------------------------------------------

-- |
data TimestampType = TimestampType
  { _TimestampType_isAdjustedToUTC :: Field 1 Bool,
    _TimestampType_unit :: Field 2 TimeUnit
  }
  deriving (Show, Eq, Generic, Pinchable, Binary)

------------------------------------------------------------------------------

-- |
data TimeType = TimeType
  { _TimeType_isAdjustedToUTC :: Field 1 Bool,
    _TimeType_unit :: Field 2 TimeUnit
  }
  deriving (Show, Eq, Generic, Pinchable, Binary)

------------------------------------------------------------------------------

-- |
data MilliSeconds = MilliSeconds
  deriving (Show, Eq, Generic, Binary)

instance Pinchable MilliSeconds where
  type Tag MilliSeconds = TStruct
  pinch _ = struct []
  unpinch _ = pure MilliSeconds

------------------------------------------------------------------------------

-- |
data MicroSeconds = MicroSeconds
  deriving (Show, Eq, Generic, Binary)

instance Pinchable MicroSeconds where
  type Tag MicroSeconds = TStruct
  pinch _ = struct []
  unpinch _ = pure MicroSeconds

------------------------------------------------------------------------------

-- |
data NanoSeconds = NanoSeconds
  deriving (Show, Eq, Generic, Binary)

instance Pinchable NanoSeconds where
  type Tag NanoSeconds = TStruct
  pinch _ = struct []
  unpinch _ = pure NanoSeconds

------------------------------------------------------------------------------

-- |
data TimeUnit
  = TimeUnitMILLIS (Field 1 MilliSeconds)
  | TimeUnitMICROS (Field 2 MicroSeconds)
  | TimeUnitNANOS (Field 3 NanoSeconds)
  deriving (Show, Eq, Generic, Binary)

instance Pinchable TimeUnit

------------------------------------------------------------------------------

-- |
data IntType = IntType
  { _IntType_bitWidth :: Field 1 Int8,
    _IntType_isSigned :: Field 2 Bool
  }
  deriving (Show, Eq, Generic, Pinchable, Binary)

------------------------------------------------------------------------------

-- |
data NullType = NullType
  deriving (Show, Eq, Generic, Binary)

instance Pinchable NullType where
  type Tag NullType = TStruct
  pinch _ = struct []
  unpinch _ = pure NullType

------------------------------------------------------------------------------

-- |
data JsonType = JsonType
  deriving (Show, Eq, Generic, Binary)

instance Pinchable JsonType where
  type Tag JsonType = TStruct
  pinch _ = struct []
  unpinch _ = pure JsonType

------------------------------------------------------------------------------

-- |
data BsonType = BsonType
  deriving (Show, Eq, Generic, Binary)

instance Pinchable BsonType where
  type Tag BsonType = TStruct
  pinch _ = struct []
  unpinch _ = pure BsonType
