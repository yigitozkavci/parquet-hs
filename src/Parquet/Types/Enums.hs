module Parquet.Types.Enums
  ( -- * Type definitions
    ColumnCryptoMetaData (..),
    ColumnOrder (..),
    CompressionCodec (..),
    ConvertedType (..),
    Encoding (..),
    EncryptionAlgorithm (..),
    FieldRepetitionType (..),
    LogicalType (..),
    PageType (..),
    Type (..),
  )
where

------------------------------------------------------------------------------

import Parquet.Prelude
import Parquet.Types.Primitives
import Pinch

------------------------------------------------------------------------------
data ColumnCryptoMetaData
  = ColumnCryptoMetaData_ENCRYPTION_WITH_FOOTER_KEY (Field 1 EncryptionWithFooterKey)
  | ColumnCryptoMetaData_ENCRYPTION_WITH_COLUMN_KEY (Field 2 EncryptionWithColumnKey)
  deriving (Show, Eq, Generic, Pinchable, Binary)

------------------------------------------------------------------------------
data ColumnOrder
  = ColumnOrder_TYPE_ORDER (Field 1 TypeDefinedOrder)
  deriving (Show, Eq, Generic, Pinchable, Binary)

------------------------------------------------------------------------------
data CompressionCodec
  = UNCOMPRESSED (Enumeration 0)
  | SNAPPY (Enumeration 1)
  | GZIP (Enumeration 2)
  | LZO (Enumeration 3)
  | BROTLI (Enumeration 4)
  | LZ4 (Enumeration 5)
  | ZSTD (Enumeration 6)
  deriving (Show, Eq, Generic, Pinchable, Binary)

------------------------------------------------------------------------------
data ConvertedType
  = UTF8 (Enumeration 0)
  | MAP (Enumeration 1)
  | MAP_KEY_VALUE (Enumeration 2)
  | LIST (Enumeration 3)
  | DECIMAL (Enumeration 5)
  | DATE (Enumeration 6)
  | TIME_MILLIS (Enumeration 7)
  | TIME_MICROS (Enumeration 8)
  | TIMESTAMP_MILLIS (Enumeration 9)
  | TIMESTAMP_MICROS (Enumeration 10)
  | UINT_8 (Enumeration 11)
  | UINT_16 (Enumeration 12)
  | UINT_32 (Enumeration 13)
  | UINT_64 (Enumeration 14)
  | INT_8 (Enumeration 15)
  | INT_16 (Enumeration 16)
  | INT_32 (Enumeration 17)
  | INT_64 (Enumeration 18)
  | JSON (Enumeration 19)
  | BSON (Enumeration 20)
  | INTERVAL (Enumeration 21)
  deriving (Show, Eq, Generic, Pinchable, Binary)

------------------------------------------------------------------------------
data Encoding
  = PLAIN (Enumeration 0)
  | PLAIN_DICTIONARY (Enumeration 2)
  | RLE (Enumeration 3)
  | BIT_PACKED (Enumeration 4)
  | DELTA_BINARY_PACKED (Enumeration 5)
  | DELTA_LENGTH_BYTE_ARRAY (Enumeration 6)
  | DELTA_BYTE_ARRAY (Enumeration 7)
  | RLE_DICTIONARY (Enumeration 8)
  deriving (Show, Eq, Generic, Pinchable, Binary)

------------------------------------------------------------------------------
data EncryptionAlgorithm
  = EncryptionAlgorithm_AES_GCM_V1 (Field 1 AesGcmV1)
  | EncryptionAlgorithm_AES_GCM_CTR_V1 (Field 2 AesGcmCtrV1)
  deriving (Show, Eq, Generic, Pinchable, Binary)

------------------------------------------------------------------------------
data FieldRepetitionType
  = REQUIRED (Enumeration 0)
  | OPTIONAL (Enumeration 1)
  | REPEATED (Enumeration 2)
  deriving (Show, Eq, Generic, Pinchable, Binary)

------------------------------------------------------------------------------
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

------------------------------------------------------------------------------
data PageType
  = DATA_PAGE (Enumeration 0)
  | INDEX_PAGE (Enumeration 1)
  | DICTIONARY_PAGE (Enumeration 2)
  | DATA_PAGE_V2 (Enumeration 3)
  deriving (Show, Eq, Generic, Pinchable, Binary)

------------------------------------------------------------------------------
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
