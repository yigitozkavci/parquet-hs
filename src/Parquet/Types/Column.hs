-- |
module Parquet.Types.Column
  ( -- * Type definitions
    ColumnChunk,
    ColumnOrder,
    ColumnMetaData (..),
  )
where

------------------------------------------------------------------------------

import Parquet.Prelude
import Parquet.Types.Common
import Parquet.Types.CompressionCodec
import Parquet.Types.Encoding
import Parquet.Types.Encryption
import Parquet.Types.KeyValue
import Parquet.Types.Page
import Parquet.Types.Statistics
import Pinch

------------------------------------------------------------------------------

-- |
data ColumnChunk = ColumnChunk
  { _ColumnChunk_file_path :: Field 1 (Maybe Text),
    _ColumnChunk_file_offset :: Field 2 Int64,
    _ColumnChunk_meta_data :: Field 3 (Maybe ColumnMetaData),
    _ColumnChunk_offset_index_offset :: Field 4 (Maybe Int64),
    _ColumnChunk_offset_index_length :: Field 5 (Maybe Int32),
    _ColumnChunk_column_index_offset :: Field 6 (Maybe Int64),
    _ColumnChunk_column_index_length :: Field 7 (Maybe Int32),
    _ColumnChunk_crypto_metadata :: Field 8 (Maybe ColumnCryptoMetaData),
    _ColumnChunk_encrypted_column_metadata :: Field 9 (Maybe ByteString)
  }
  deriving (Show, Eq, Generic, Pinchable, Binary)

------------------------------------------------------------------------------

-- |
data ColumnMetaData = ColumnMetaData
  { _ColumnMetaData_type :: Field 1 Type,
    _ColumnMetaData_encodings :: Field 2 [Encoding],
    _ColumnMetaData_path_in_schema :: Field 3 [Text],
    _ColumnMetaData_codec :: Field 4 CompressionCodec,
    _ColumnMetaData_num_values :: Field 5 Int64,
    _ColumnMetaData_total_uncompressed_size :: Field 6 Int64,
    _ColumnMetaData_total_compressed_size :: Field 7 Int64,
    _ColumnMetaData_key_value_metadata :: Field 8 (Maybe [KeyValue]),
    _ColumnMetaData_data_page_offset :: Field 9 Int64,
    _ColumnMetaData_index_page_offset :: Field 10 (Maybe Int64),
    _ColumnMetaData_dictionary_page_offset :: Field 11 (Maybe Int64),
    _ColumnMetaData_statistics :: Field 12 (Maybe Statistics),
    _ColumnMetaData_encoding_stats :: Field 13 (Maybe [PageEncodingStats]),
    _ColumnMetaData_bloom_filter_offset :: Field 14 (Maybe Int64)
  }
  deriving (Show, Eq, Generic, Pinchable, Binary)

------------------------------------------------------------------------------

-- |
data ColumnCryptoMetaData
  = ColumnCryptoMetaData_ENCRYPTION_WITH_FOOTER_KEY (Field 1 EncryptionWithFooterKey)
  | ColumnCryptoMetaData_ENCRYPTION_WITH_COLUMN_KEY (Field 2 EncryptionWithColumnKey)
  deriving (Show, Eq, Generic, Pinchable, Binary)

------------------------------------------------------------------------------

-- |
data ColumnOrder
  = ColumnOrder_TYPE_ORDER (Field 1 TypeDefinedOrder)
  deriving (Show, Eq, Generic, Pinchable, Binary)

------------------------------------------------------------------------------

-- |
data TypeDefinedOrder = TypeDefinedOrder
  deriving (Show, Eq, Generic, Binary)

instance Pinchable TypeDefinedOrder where
  type Tag TypeDefinedOrder = TStruct
  pinch _ = struct []
  unpinch _ = pure TypeDefinedOrder
