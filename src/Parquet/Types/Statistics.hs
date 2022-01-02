-- |
module Parquet.Types.Statistics
  ( -- * Type definitions
    Statistics,
  )
where

------------------------------------------------------------------------------

import Parquet.Prelude
import Pinch

------------------------------------------------------------------------------

-- |
data Statistics = Statistics
  { _Statistics_max :: Field 1 (Maybe ByteString),
    _Statistics_min :: Field 2 (Maybe ByteString),
    _Statistics_null_count :: Field 3 (Maybe Int64),
    _Statistics_distinct_count :: Field 4 (Maybe Int64),
    _Statistics_max_value :: Field 5 (Maybe ByteString),
    _Statistics_min_value :: Field 6 (Maybe ByteString)
  }
  deriving (Show, Eq, Generic, Pinchable, Binary)
