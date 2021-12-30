-- |
module Parquet.Types.KeyValue
  ( KeyValue (..),
  )
where

------------------------------------------------------------------------------

import Data.Binary (Binary)
import Data.Text
import GHC.Generics
import Parquet.Types.Statistics ()
import Pinch

------------------------------------------------------------------------------

-- |
data KeyValue = KeyValue
  { _KeyValue_key :: Field 1 Text,
    _KeyValue_value :: Field 2 (Maybe Text)
  }
  deriving (Show, Eq, Generic, Pinchable, Binary)
