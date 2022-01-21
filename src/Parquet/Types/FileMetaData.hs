module Parquet.Types.FileMetaData
  ( -- * Type definitions
    FileMetadata (..),
    SchemaElement (..),
  )
where

------------------------------------------------------------------------------

import Parquet.Prelude
import Parquet.Types.Enums
import Parquet.Types.Primitives
import Parquet.Types.RowGroup
import Pinch

------------------------------------------------------------------------------
data FileMetadata = FileMetadata
  { _FileMetadata_version :: Field 1 Int32,
    _FileMetadata_schema :: Field 2 [SchemaElement],
    _FileMetadata_num_rows :: Field 3 Int64,
    _FileMetadata_row_groups :: Field 4 [RowGroup],
    _FileMetadata_key_value_metadata :: Field 5 (Maybe [KeyValue]),
    _FileMetadata_created_by :: Field 6 (Maybe Text),
    _FileMetadata_column_orders :: Field 7 (Maybe [ColumnOrder]),
    _FileMetadata_encryption_algorithm :: Field 8 (Maybe EncryptionAlgorithm),
    _FileMetadata_footer_signing_key_metadata :: Field 9 (Maybe ByteString)
  }
  deriving (Show, Eq, Generic, Pinchable, Binary)

------------------------------------------------------------------------------
data SchemaElement = SchemaElement
  { _SchemaElement_type :: Field 1 (Maybe Type),
    _SchemaElement_type_length :: Field 2 (Maybe Int32),
    _SchemaElement_repetition_type :: Field 3 (Maybe FieldRepetitionType),
    _SchemaElement_name :: Field 4 Text,
    _SchemaElement_num_children :: Field 5 (Maybe Int32),
    _SchemaElement_converted_type :: Field 6 (Maybe ConvertedType),
    _SchemaElement_scale :: Field 7 (Maybe Int32),
    _SchemaElement_precision :: Field 8 (Maybe Int32),
    _SchemaElement_field_id :: Field 9 (Maybe Int32),
    _SchemaElement_logicalType :: Field 10 (Maybe LogicalType)
  }
  deriving (Show, Eq, Generic, Pinchable, Binary)
