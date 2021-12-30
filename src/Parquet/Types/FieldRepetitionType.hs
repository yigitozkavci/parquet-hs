-- |
module Parquet.Types.FieldRepetitionType where

------------------------------------------------------------------------------

import Data.Binary (Binary)
import GHC.Generics
import Parquet.Types.Common ()
import Pinch

------------------------------------------------------------------------------

-- |
data FieldRepetitionType
  = REQUIRED (Enumeration 0)
  | OPTIONAL (Enumeration 1)
  | REPEATED (Enumeration 2)
  deriving (Show, Eq, Generic, Pinchable, Binary)
