-- |
module Parquet.Types.KeyValue
  ( KeyValue (..),
  )
where

------------------------------------------------------------------------------

import Parquet.Prelude
import Pinch

------------------------------------------------------------------------------

-- |
data KeyValue = KeyValue
  { _KeyValue_key :: Field 1 Text,
    _KeyValue_value :: Field 2 (Maybe Text)
  }
  deriving (Show, Eq, Generic, Pinchable, Binary)
