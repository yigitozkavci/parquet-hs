-- |
module Parquet.Types.RowGroup
  ( RowGroup,
  )
where

------------------------------------------------------------------------------

import Parquet.Prelude
import Parquet.Types.Column
import Parquet.Types.SortingColumn
import Pinch

------------------------------------------------------------------------------

-- |
data RowGroup = RowGroup
  { _RowGroup_column_chunks :: Field 1 [ColumnChunk],
    _RowGroup_total_byte_size :: Field 2 Int64,
    _RowGroup_num_rows :: Field 3 Int64,
    _RowGroup_sorting_columns :: Field 4 (Maybe [SortingColumn]),
    _RowGroup_file_offset :: Field 5 (Maybe Int64),
    _RowGroup_total_compressed_size :: Field 6 (Maybe Int64),
    _RowGroup_ordinal :: Field 7 (Maybe Int16)
  }
  deriving (Show, Eq, Generic, Binary, Pinchable)
