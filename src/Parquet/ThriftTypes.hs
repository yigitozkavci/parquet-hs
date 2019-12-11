{-# LANGUAGE AllowAmbiguousTypes  #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DataKinds            #-}
{-# LANGUAGE DeriveGeneric        #-}
{-# LANGUAGE FlexibleContexts     #-}
{-# LANGUAGE RankNTypes           #-}
{-# LANGUAGE ScopedTypeVariables  #-}
{-# LANGUAGE TypeApplications     #-}
{-# LANGUAGE TypeFamilies         #-}
{-# LANGUAGE TypeOperators        #-}
{-# LANGUAGE UndecidableInstances #-}

module Parquet.ThriftTypes where

import Data.ByteString
import qualified Data.Generics.Product.Fields as GL
import qualified Data.Generics.Product.Positions as GL
import Data.Int
import Data.Text
import GHC.Generics
import GHC.TypeLits (AppendSymbol, Symbol)
import Lens.Micro
import Pinch
import Data.Binary (Binary)

-- TODO(yigitozkavci): Move these orphan instances to the +pinch library code.
-- This will require opening a PR to https://github.com/abhinav/pinch.
instance Binary a => Binary (Field k a)
instance Binary (Enumeration k)

data StringType = StringType
  deriving (Show, Eq, Generic, Binary)
instance Pinchable StringType where
  type Tag StringType = TStruct
  pinch _ = struct []
  unpinch _ = pure StringType

data UUIDType = UUIDType
  deriving (Show, Eq, Generic, Binary)
instance Pinchable UUIDType where
  type Tag UUIDType = TStruct
  pinch _ = struct []
  unpinch _ = pure UUIDType

data MapType = MapType
  deriving (Show, Eq, Generic, Binary)
instance Pinchable MapType where
  type Tag MapType = TStruct
  pinch _ = struct []
  unpinch _ = pure MapType

data ListType = ListType
  deriving (Show, Eq, Generic, Binary)
instance Pinchable ListType where
  type Tag ListType = TStruct
  pinch _ = struct []
  unpinch _ = pure ListType

data EnumType = EnumType
  deriving (Show, Eq, Generic, Binary)
instance Pinchable EnumType where
  type Tag EnumType = TStruct
  pinch _ = struct []
  unpinch _ = pure EnumType

data DateType = DateType
  deriving (Show, Eq, Generic, Binary)
instance Pinchable DateType where
  type Tag DateType = TStruct
  pinch _ = struct []
  unpinch _ = pure DateType

type family TypeName a :: Symbol where
  TypeName (M1 D ('MetaData name _ _ _) f ()) = name
  TypeName a = TypeName (Rep a ())

pinchPos
  :: forall pos s t a1 b1 a2 b2
   . (GL.HasPosition 1 a1 b1 a2 b2, GL.HasPosition pos s t a1 b1)
  => Lens s t a2 b2
pinchPos = GL.position @pos . GL.position @1

pinchField
  :: forall field s i r field_name
   . ( field_name ~ ("_" `AppendSymbol` TypeName s `AppendSymbol` "_" `AppendSymbol` field)
     , GL.HasPosition 1 i i r r
     , GL.HasField field_name s s i i
     )
  => Lens s s r r
pinchField = GL.field @field_name . GL.position @1

data DecimalType = DecimalType
  { _DecimalType_scale     :: Field 1 Int32
  , _DecimalType_precision :: Field 2 Int32
  } deriving (Show, Eq, Generic, Pinchable, Binary)

data TimestampType = TimestampType
  { _TimestampType_isAdjustedToUTC :: Field 1 Bool
  , _TimestampType_unit            :: Field 2 TimeUnit
  } deriving (Show, Eq, Generic, Pinchable, Binary)

data TimeType = TimeType
  { _TimeType_isAdjustedToUTC :: Field 1 Bool
  , _TimeType_unit            :: Field 2 TimeUnit
  } deriving (Show, Eq, Generic, Pinchable, Binary)

data MilliSeconds = MilliSeconds
  deriving (Show, Eq, Generic, Binary)
instance Pinchable MilliSeconds where
  type Tag MilliSeconds = TStruct
  pinch _ = struct []
  unpinch _ = pure MilliSeconds

data MicroSeconds = MicroSeconds
  deriving (Show, Eq, Generic, Binary)
instance Pinchable MicroSeconds where
  type Tag MicroSeconds = TStruct
  pinch _ = struct []
  unpinch _ = pure MicroSeconds

data NanoSeconds = NanoSeconds
  deriving (Show, Eq, Generic, Binary)
instance Pinchable NanoSeconds where
  type Tag NanoSeconds = TStruct
  pinch _ = struct []
  unpinch _ = pure NanoSeconds

data TimeUnit
  = TimeUnitMILLIS (Field 1 MilliSeconds)
  | TimeUnitMICROS (Field 2 MicroSeconds)
  | TimeUnitNANOS (Field 3 NanoSeconds)
  deriving (Show, Eq, Generic, Binary)
instance Pinchable TimeUnit

data IntType = IntType
  { _IntType_bitWidth :: Field 1 Int8
  , _IntType_isSigned :: Field 2 Bool
  } deriving (Show, Eq, Generic, Pinchable, Binary)

data NullType = NullType
  deriving (Show, Eq, Generic, Binary)
instance Pinchable NullType where
  type Tag NullType = TStruct
  pinch _ = struct []
  unpinch _ = pure NullType

data JsonType = JsonType
  deriving (Show, Eq, Generic, Binary)
instance Pinchable JsonType where
  type Tag JsonType = TStruct
  pinch _ = struct []
  unpinch _ = pure JsonType

data BsonType = BsonType
  deriving (Show, Eq, Generic, Binary)
instance Pinchable BsonType where
  type Tag BsonType = TStruct
  pinch _ = struct []
  unpinch _ = pure BsonType

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

data FieldRepetitionType
  = REQUIRED (Enumeration 0)
  | OPTIONAL (Enumeration 1)
  | REPEATED (Enumeration 2)
  deriving (Show, Eq, Generic, Pinchable, Binary)

data SchemaElement = SchemaElement
  { _SchemaElement_type            :: Field 1 (Maybe Type)
  , _SchemaElement_type_length     :: Field 2 (Maybe Int32)
  , _SchemaElement_repetition_type :: Field 3 (Maybe FieldRepetitionType)
  , _SchemaElement_name            :: Field 4 Text
  , _SchemaElement_num_children    :: Field 5 (Maybe Int32)
  , _SchemaElement_converted_type  :: Field 6 (Maybe ConvertedType)
  , _SchemaElement_scale           :: Field 7 (Maybe Int32)
  , _SchemaElement_precision       ::  Field 8 (Maybe Int32)
  , _SchemaElement_field_id        :: Field 9 (Maybe Int32)
  , _SchemaElement_logicalType     :: Field 10 (Maybe LogicalType)
  }
  deriving (Show, Eq, Generic, Pinchable, Binary)

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

data CompressionCodec
  = UNCOMPRESSED (Enumeration 0)
  | SNAPPY (Enumeration 1)
  | GZIP (Enumeration 2)
  | LZO (Enumeration 3)
  | BROTLI (Enumeration 4)
  | LZ4 (Enumeration 5)
  | ZSTD (Enumeration 6)
  deriving (Show, Eq, Generic, Pinchable, Binary)

data Statistics = Statistics
   { _Statistics_max            :: Field 1 (Maybe ByteString)
   , _Statistics_min            :: Field 2 (Maybe ByteString)
   , _Statistics_null_count     :: Field 3 (Maybe Int64)
   , _Statistics_distinct_count :: Field 4 (Maybe Int64)
   , _Statistics_max_value      :: Field 5 (Maybe ByteString)
   , _Statistics_min_value      :: Field 6 (Maybe ByteString)
  } deriving (Show, Eq, Generic, Pinchable, Binary)

data PageEncodingStats = PageEncodingStats
  { _PageEncodingStats_page_type :: Field 1 PageType
  , _PageEncodingStats_encoding  :: Field 2 Encoding
  , _PageEncodingStats_count     :: Field 3 Int32
  } deriving (Show, Eq, Generic, Pinchable, Binary)

data PageType
  = DATA_PAGE (Enumeration 0)
  | INDEX_PAGE (Enumeration 1)
  | DICTIONARY_PAGE (Enumeration 2)
  | DATA_PAGE_V2 (Enumeration 3)
  deriving (Show, Eq, Generic, Pinchable, Binary)

data SortingColumn = SortingColumn
  { _SortingColumn_column_idx  :: Field 1 Int32
  , _SortingColumn_descending  :: Field 2 Bool
  , _SortingColumn_nulls_first :: Field 3 Bool
  } deriving (Show, Eq, Generic, Pinchable, Binary)

data AesGcmV1 = AesGcmV1
  { _AesGcmV1_aad_prefix        :: Field 1 (Maybe ByteString)
  , _AesGcmV1_aad_file_unique   :: Field 2 (Maybe ByteString)
  , _AesGcmV1_supply_aad_prefix :: Field 3 (Maybe Bool)
  } deriving (Show, Eq, Generic, Pinchable, Binary)

data AesGcmCtrV1 = AesGcmCtrV1
  { _AesGcmCtrV1_aad_prefix        :: Field 1 (Maybe ByteString)
  , _AesGcmCtrV1_aad_file_unique   :: Field 2 (Maybe ByteString)
  , _AesGcmCtrV1_supply_aad_prefix :: Field 3 (Maybe Bool)
  } deriving (Show, Eq, Generic, Pinchable, Binary)

data EncryptionAlgorithm
  = EncryptionAlgorithm_AES_GCM_V1 (Field 1 AesGcmV1)
  | EncryptionAlgorithm_AES_GCM_CTR_V1 (Field 2 AesGcmCtrV1)
  deriving (Show, Eq, Generic, Pinchable, Binary)

data TypeDefinedOrder = TypeDefinedOrder
  deriving (Show, Eq, Generic, Binary)
instance Pinchable TypeDefinedOrder where
  type Tag TypeDefinedOrder = TStruct
  pinch _ = struct []
  unpinch _ = pure TypeDefinedOrder

data ColumnOrder
  = ColumnOrder_TYPE_ORDER (Field 1 TypeDefinedOrder)
  deriving (Show, Eq, Generic, Pinchable, Binary)

data EncryptionWithFooterKey = EncryptionWithFooterKey
  deriving (Show, Eq, Generic, Binary)
instance Pinchable EncryptionWithFooterKey where
  type Tag EncryptionWithFooterKey = TStruct
  pinch _ = struct []
  unpinch _ = pure EncryptionWithFooterKey

data EncryptionWithColumnKey = EncryptionWithColumnKey
  { _EncryptionWithColumnKey_path_in_schema :: Field 1 [Text]
  , _EncryptionWithColumnKey_key_metadata   :: Field 2 (Maybe ByteString)
  } deriving (Show, Eq, Generic, Pinchable, Binary)

data ColumnCryptoMetaData
  = ColumnCryptoMetaData_ENCRYPTION_WITH_FOOTER_KEY (Field 1 EncryptionWithFooterKey)
  | ColumnCryptoMetaData_ENCRYPTION_WITH_COLUMN_KEY (Field 2 EncryptionWithColumnKey)
  deriving (Show, Eq, Generic, Pinchable, Binary)

data KeyValue = KeyValue
  { _KeyValue_key   :: Field 1 Text
  , _KeyValue_value :: Field 2 (Maybe Text)
  } deriving (Show, Eq, Generic, Pinchable, Binary)

data ColumnMetaData = ColumnMetaData
  { _ColumnMetaData_type :: Field 1 Type
  , _ColumnMetaData_encodings :: Field 2 [Encoding]
  , _ColumnMetaData_path_in_schema :: Field 3 [Text]
  , _ColumnMetaData_codec :: Field 4 CompressionCodec
  , _ColumnMetaData_num_values :: Field 5 Int64
  , _ColumnMetaData_total_uncompressed_size :: Field 6 Int64
  , _ColumnMetaData_total_compressed_size :: Field 7 Int64
  , _ColumnMetaData_key_value_metadata :: Field 8 (Maybe [KeyValue])
  , _ColumnMetaData_data_page_offset :: Field 9 Int64
  , _ColumnMetaData_index_page_offset :: Field 10 (Maybe Int64)
  , _ColumnMetaData_dictionary_page_offset :: Field 11 (Maybe Int64)
  , _ColumnMetaData_statistics :: Field 12 (Maybe Statistics)
  , _ColumnMetaData_encoding_stats :: Field 13 (Maybe [PageEncodingStats])
  , _ColumnMetaData_bloom_filter_offset :: Field 14 (Maybe Int64)
  } deriving (Show, Eq, Generic, Pinchable, Binary)

data ColumnChunk = ColumnChunk
  { _ColumnChunk_file_path :: Field 1 (Maybe Text)
  , _ColumnChunk_file_offset :: Field 2 Int64
  , _ColumnChunk_meta_data :: Field 3 (Maybe ColumnMetaData)
  , _ColumnChunk_offset_index_offset :: Field 4 (Maybe Int64)
  , _ColumnChunk_offset_index_length :: Field 5 (Maybe Int32)
  , _ColumnChunk_column_index_offset :: Field 6 (Maybe Int64)
  , _ColumnChunk_column_index_length :: Field 7 (Maybe Int32)
  , _ColumnChunk_crypto_metadata :: Field 8 (Maybe ColumnCryptoMetaData)
  , _ColumnChunk_encrypted_column_metadata :: Field 9 (Maybe ByteString)
  } deriving (Show, Eq, Generic, Pinchable, Binary)

data RowGroup = RowGroup
  { _RowGroup_column_chunks         :: Field 1 [ColumnChunk]
  , _RowGroup_total_byte_size       :: Field 2 Int64
  , _RowGroup_num_rows              :: Field 3 Int64
  , _RowGroup_sorting_columns       :: Field 4 (Maybe [SortingColumn])
  , _RowGroup_file_offset           :: Field 5 (Maybe Int64)
  , _RowGroup_total_compressed_size :: Field 6 (Maybe Int64)
  , _RowGroup_ordinal               :: Field 7 (Maybe Int16)
  } deriving (Show, Eq, Generic, Binary, Pinchable)

data FileMetadata = FileMetadata
    { _FileMetadata_version                     :: Field 1 Int32
    , _FileMetadata_schema                      :: Field 2 [SchemaElement]
    , _FileMetadata_num_rows                    :: Field 3 Int64
    , _FileMetadata_row_groups                  :: Field 4 [RowGroup]
    , _FileMetadata_key_value_metadata          :: Field 5 (Maybe [KeyValue])
    , _FileMetadata_created_by                  :: Field 6 (Maybe Text)
    , _FileMetadata_column_orders               :: Field 7 (Maybe [ColumnOrder])
    , _FileMetadata_encryption_algorithm        :: Field 8 (Maybe EncryptionAlgorithm)
    , _FileMetadata_footer_signing_key_metadata :: Field 9 (Maybe ByteString)
    } deriving (Show, Eq, Generic, Pinchable, Binary)

data PageHeader = PageHeader
  { _PageHeader_type                   :: Field 1 PageType
  , _PageHeader_uncompressed_page_size :: Field 2 Int32
  , _PageHeader_compressed_page_size   :: Field 3 Int32
  , _PageHeader_crc                    :: Field 4 (Maybe Int32)
  , _PageHeader_data_page_header       :: Field 5 (Maybe DataPageHeader)
  , _PageHeader_index_page_header      :: Field 6 (Maybe IndexPageHeader)
  , _PageHeader_dictionary_page_header :: Field 7 (Maybe DictionaryPageHeader)
  , _PageHeader_data_page_header_v2    :: Field 8 (Maybe DataPageHeaderV2)
  } deriving (Show, Eq, Generic, Pinchable, Binary)

data IndexPageHeader = IndexPageHeader
  deriving (Show, Eq, Generic, Binary)
instance Pinchable IndexPageHeader where
  type Tag IndexPageHeader = TStruct
  pinch _ = struct []
  unpinch _ = pure IndexPageHeader

data DataPageHeader = DataPageHeader
  { _DataPageHeader_num_values                :: Field 1 Int32
  , _DataPageHeader_encoding                  :: Field 2 Encoding
  , _DataPageHeader_definition_level_encoding :: Field 3 Encoding
  , _DataPageHeader_repetition_level_encoding :: Field 4 Encoding
  , _DataPageHeader_statistics                :: Field 5 (Maybe Statistics)
  } deriving (Show, Eq, Generic, Pinchable, Binary)

data DictionaryPageHeader = DictionaryPageHeader
  { _DictionaryPageHeader_num_values :: Field 1 Int32
  , _DictionaryPageHeader_encoding   :: Field 2 Encoding
  , _DictionaryPageHeader_is_sorted  :: Field 3 (Maybe Bool)
  } deriving (Show, Eq, Generic, Pinchable, Binary)

data DataPageHeaderV2 = DataPageHeaderV2
  { _DataPageHeaderV2_num_values :: Field 1 Int32
  , _DataPageHeaderV2_num_nulls :: Field 2 Int32
  , _DataPageHeaderV2_num_rows :: Field 3 Int32
  , _DataPageHeaderV2_encoding :: Field 4 Encoding
  , _DataPageHeaderV2_definition_levels_byte_length :: Field 5 Int32
  , _DataPageHeaderV2_repetition_levels_byte_length :: Field 6 Int32
  , _DataPageHeaderV2_is_compressed :: Field 7 (Maybe Bool)
  , _DataPageHeaderV2_statistics :: Field 8 (Maybe Statistics)
  } deriving (Show, Eq, Generic, Pinchable, Binary)

unField :: Field n a -> a
unField (Field a) = a
