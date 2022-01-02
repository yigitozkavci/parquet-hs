-- |
module Parquet.Types.FileMetadata
  ( -- * Type definitions
    FileMetadata,
  )
where

------------------------------------------------------------------------------

import Parquet.Prelude
import Parquet.Types.Column
import Parquet.Types.Encryption
import Parquet.Types.KeyValue
import Parquet.Types.RowGroup
import Parquet.Types.SchemaElement
import Pinch

------------------------------------------------------------------------------

-- |
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
