-- |
module Parquet.Types.CompressionCodec where

------------------------------------------------------------------------------

import Data.Binary (Binary)
import GHC.Generics
import Parquet.Types.Statistics ()
import Pinch

------------------------------------------------------------------------------

-- |
data CompressionCodec
  = UNCOMPRESSED (Enumeration 0)
  | SNAPPY (Enumeration 1)
  | GZIP (Enumeration 2)
  | LZO (Enumeration 3)
  | BROTLI (Enumeration 4)
  | LZ4 (Enumeration 5)
  | ZSTD (Enumeration 6)
  deriving (Show, Eq, Generic, Pinchable, Binary)
