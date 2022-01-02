-- |
module Parquet.Types.Encoding
  ( Encoding (..),
  )
where

------------------------------------------------------------------------------

import Parquet.Prelude
import Pinch

------------------------------------------------------------------------------

-- |
data Encoding
  = PLAIN (Enumeration 0)
  | PLAIN_DICTIONARY (Enumeration 2)
  | RLE (Enumeration 3)
  | BIT_PACKED (Enumeration 4)
  | DELTA_BINARY_PACKED (Enumeration 5)
  | DELTA_LENGTH_BYTE_ARRAY (Enumeration 6)
  | DELTA_BYTE_ARRAY (Enumeration 7)
  | RLE_DICTIONARY (Enumeration 8)
  deriving (Show, Eq, Generic, Pinchable, Binary)
