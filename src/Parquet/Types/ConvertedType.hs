-- |
module Parquet.Types.ConvertedType
  ( -- * Type definitions
    ConvertedType,
  )
where

------------------------------------------------------------------------------

import Parquet.Prelude
import Pinch

------------------------------------------------------------------------------

-- |
data ConvertedType
  = UTF8 (Enumeration 0)
  | MAP (Enumeration 1)
  | MAP_KEY_VALUE (Enumeration 2)
  | LIST (Enumeration 3)
  | DECIMAL (Enumeration 5)
  | DATE (Enumeration 6)
  | TIME_MILLIS (Enumeration 7)
  | TIME_MICROS (Enumeration 8)
  | TIMESTAMP_MILLIS (Enumeration 9)
  | TIMESTAMP_MICROS (Enumeration 10)
  | UINT_8 (Enumeration 11)
  | UINT_16 (Enumeration 12)
  | UINT_32 (Enumeration 13)
  | UINT_64 (Enumeration 14)
  | INT_8 (Enumeration 15)
  | INT_16 (Enumeration 16)
  | INT_32 (Enumeration 17)
  | INT_64 (Enumeration 18)
  | JSON (Enumeration 19)
  | BSON (Enumeration 20)
  | INTERVAL (Enumeration 21)
  deriving (Show, Eq, Generic, Pinchable, Binary)
