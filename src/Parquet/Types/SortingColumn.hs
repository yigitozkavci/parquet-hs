-- |
module Parquet.Types.SortingColumn where

------------------------------------------------------------------------------

import Data.Binary (Binary)
import Data.Int
import GHC.Generics
import Parquet.Types.AesGcm ()
import Pinch

------------------------------------------------------------------------------

-- |
data SortingColumn = SortingColumn
  { _SortingColumn_column_idx :: Field 1 Int32,
    _SortingColumn_descending :: Field 2 Bool,
    _SortingColumn_nulls_first :: Field 3 Bool
  }
  deriving (Show, Eq, Generic, Pinchable, Binary)
