module Parquet.Types.PageHeader
  ( -- * Type definitions
    DataPageHeader (..),
    DictionaryPageHeader (..),
    PageHeader (..),

    -- * Internal type definitions
    IndexPageHeader (..),

    -- * Unused type definitions
    DataPageHeaderV2 (..),
  )
where

------------------------------------------------------------------------------

import Parquet.Prelude
import Parquet.Types.Enums
import Parquet.Types.Primitives
import Pinch

------------------------------------------------------------------------------
data DataPageHeader = DataPageHeader
  { _DataPageHeader_num_values :: Field 1 Int32,
    _DataPageHeader_encoding :: Field 2 Encoding,
    _DataPageHeader_definition_level_encoding :: Field 3 Encoding,
    _DataPageHeader_repetition_level_encoding :: Field 4 Encoding,
    _DataPageHeader_statistics :: Field 5 (Maybe Statistics)
  }
  deriving (Show, Eq, Generic, Pinchable, Binary)

------------------------------------------------------------------------------
data DataPageHeaderV2 = DataPageHeaderV2
  { _DataPageHeaderV2_num_values :: Field 1 Int32,
    _DataPageHeaderV2_num_nulls :: Field 2 Int32,
    _DataPageHeaderV2_num_rows :: Field 3 Int32,
    _DataPageHeaderV2_encoding :: Field 4 Encoding,
    _DataPageHeaderV2_definition_levels_byte_length :: Field 5 Int32,
    _DataPageHeaderV2_repetition_levels_byte_length :: Field 6 Int32,
    _DataPageHeaderV2_is_compressed :: Field 7 (Maybe Bool),
    _DataPageHeaderV2_statistics :: Field 8 (Maybe Statistics)
  }
  deriving (Show, Eq, Generic, Pinchable, Binary)

------------------------------------------------------------------------------
data DictionaryPageHeader = DictionaryPageHeader
  { _DictionaryPageHeader_num_values :: Field 1 Int32,
    _DictionaryPageHeader_encoding :: Field 2 Encoding,
    _DictionaryPageHeader_is_sorted :: Field 3 (Maybe Bool)
  }
  deriving (Show, Eq, Generic, Pinchable, Binary)

------------------------------------------------------------------------------
data IndexPageHeader = IndexPageHeader
  deriving (Show, Eq, Generic, Binary)

instance Pinchable IndexPageHeader where
  type Tag IndexPageHeader = TStruct
  pinch _ = struct []
  unpinch _ = pure IndexPageHeader

------------------------------------------------------------------------------
data PageHeader = PageHeader
  { _PageHeader_type :: Field 1 PageType,
    _PageHeader_uncompressed_page_size :: Field 2 Int32,
    _PageHeader_compressed_page_size :: Field 3 Int32,
    _PageHeader_crc :: Field 4 (Maybe Int32),
    _PageHeader_data_page_header :: Field 5 (Maybe DataPageHeader),
    _PageHeader_index_page_header :: Field 6 (Maybe IndexPageHeader),
    _PageHeader_dictionary_page_header :: Field 7 (Maybe DictionaryPageHeader),
    _PageHeader_data_page_header_v2 :: Field 8 (Maybe DataPageHeaderV2)
  }
  deriving (Show, Eq, Generic, Pinchable, Binary)
