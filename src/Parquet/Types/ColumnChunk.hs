module Parquet.Types.ColumnChunk
  ( -- * Type definitions
    ColumnChunk (..),
    ColumnMetaData (..),

    -- * Internal type definitions
    PageEncodingStats (..),
  )
where

------------------------------------------------------------------------------

import Parquet.Prelude
import Parquet.Types.Enums
import Parquet.Types.Primitives
import Pinch

------------------------------------------------------------------------------
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
data PageEncodingStats = PageEncodingStats
  { _PageEncodingStats_page_type :: Field 1 PageType,
    _PageEncodingStats_encoding :: Field 2 Encoding,
    _PageEncodingStats_count :: Field 3 Int32
  }
  deriving (Show, Eq, Generic, Pinchable, Binary)
