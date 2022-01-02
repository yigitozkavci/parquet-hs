-- |
module Parquet.Types.SortingColumn
  ( -- * Type definitions
    SortingColumn
  )
where

------------------------------------------------------------------------------

import Parquet.Prelude
import Pinch

------------------------------------------------------------------------------

-- |
data SortingColumn = SortingColumn
  { _SortingColumn_column_idx :: Field 1 Int32,
    _SortingColumn_descending :: Field 2 Bool,
    _SortingColumn_nulls_first :: Field 3 Bool
  }
  deriving (Show, Eq, Generic, Pinchable, Binary)
