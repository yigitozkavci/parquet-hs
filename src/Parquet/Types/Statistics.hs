-- |
module Parquet.Types.Statistics
  ( Statistics (..),
  )
where

------------------------------------------------------------------------------

import Data.Binary (Binary)
import Data.ByteString
import Data.Int
import GHC.Generics
import Parquet.Types.Encoding ()
import Pinch

------------------------------------------------------------------------------

-- TODO(yigitozkavci): Move these orphan instances to the +pinch library code.
-- This will require opening a PR to https://github.com/abhinav/pinch.
instance Binary a => Binary (Field k a)

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
