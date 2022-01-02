-- |
module Parquet.Types.FieldRepetitionType
  ( -- * Type definitions
    FieldRepetitionType (..),
  )
where

------------------------------------------------------------------------------

import Parquet.Prelude
import Pinch

------------------------------------------------------------------------------

-- |
data FieldRepetitionType
  = REQUIRED (Enumeration 0)
  | OPTIONAL (Enumeration 1)
  | REPEATED (Enumeration 2)
  deriving (Show, Eq, Generic, Pinchable, Binary)
