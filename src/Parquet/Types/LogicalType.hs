-- |
module Parquet.Types.LogicalType where

------------------------------------------------------------------------------

import Data.Binary (Binary)
import GHC.Generics
import Parquet.Types.Common
import Pinch

------------------------------------------------------------------------------

-- |
data LogicalType
  = LogicalTypeSTRING (Field 1 StringType)
  | LogicalTypeMAP (Field 2 MapType)
  | LogicalTypeLIST (Field 3 ListType)
  | LogicalTypeENUM (Field 4 EnumType)
  | LogicalTypeDECIMAL (Field 5 DecimalType)
  | LogicalTypeDATE (Field 6 DateType)
  | LogicalTypeTIME (Field 7 TimeType)
  | LogicalTypeTIMESTAMP (Field 8 TimestampType)
  | LogicalTypeINTEGER (Field 10 IntType)
  | LogicalTypeUNKNOWN (Field 11 NullType)
  | LogicalTypeJSON (Field 12 JsonType)
  | LogicalTypeBSON (Field 13 BsonType)
  | LogicalTypeUUID (Field 14 UUIDType)
  deriving (Show, Eq, Generic, Pinchable, Binary)
