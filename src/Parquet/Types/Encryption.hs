-- |
module Parquet.Types.Encryption
  ( -- * Type definitions
    EncryptionAlgorithm,
    EncryptionWithFooterKey,
    EncryptionWithColumnKey,
  )
where

------------------------------------------------------------------------------

import Parquet.Prelude
import Parquet.Types.AesGcm
import Pinch

------------------------------------------------------------------------------

-- |
data EncryptionAlgorithm
  = EncryptionAlgorithm_AES_GCM_V1 (Field 1 AesGcmV1)
  | EncryptionAlgorithm_AES_GCM_CTR_V1 (Field 2 AesGcmCtrV1)
  deriving (Show, Eq, Generic, Pinchable, Binary)

------------------------------------------------------------------------------

-- |
data EncryptionWithFooterKey = EncryptionWithFooterKey
  deriving (Show, Eq, Generic, Binary)

instance Pinchable EncryptionWithFooterKey where
  type Tag EncryptionWithFooterKey = TStruct
  pinch _ = struct []
  unpinch _ = pure EncryptionWithFooterKey

------------------------------------------------------------------------------

-- |
data EncryptionWithColumnKey = EncryptionWithColumnKey
  { _EncryptionWithColumnKey_path_in_schema :: Field 1 [Text],
    _EncryptionWithColumnKey_key_metadata :: Field 2 (Maybe ByteString)
  }
  deriving (Show, Eq, Generic, Pinchable, Binary)
