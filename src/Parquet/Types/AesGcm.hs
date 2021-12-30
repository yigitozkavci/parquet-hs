-- |
module Parquet.Types.AesGcm where

------------------------------------------------------------------------------

import Data.Binary (Binary)
import Data.ByteString
import GHC.Generics
import Parquet.Types.Statistics ()
import Pinch

------------------------------------------------------------------------------

-- |
data AesGcmV1 = AesGcmV1
  { _AesGcmV1_aad_prefix :: Field 1 (Maybe ByteString),
    _AesGcmV1_aad_file_unique :: Field 2 (Maybe ByteString),
    _AesGcmV1_supply_aad_prefix :: Field 3 (Maybe Bool)
  }
  deriving (Show, Eq, Generic, Pinchable, Binary)

------------------------------------------------------------------------------

-- |
data AesGcmCtrV1 = AesGcmCtrV1
  { _AesGcmCtrV1_aad_prefix :: Field 1 (Maybe ByteString),
    _AesGcmCtrV1_aad_file_unique :: Field 2 (Maybe ByteString),
    _AesGcmCtrV1_supply_aad_prefix :: Field 3 (Maybe Bool)
  }
  deriving (Show, Eq, Generic, Pinchable, Binary)
