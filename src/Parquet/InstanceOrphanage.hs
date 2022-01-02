{-# OPTIONS_GHC -fno-warn-orphans #-}

-- |
module Parquet.InstanceOrphanage where

------------------------------------------------------------------------------

import Data.Binary (Binary)
import Pinch

------------------------------------------------------------------------------
-- TODO(yigitozkavci): Move these orphan instances to the +pinch library code.
-- This will require opening a PR to https://github.com/abhinav/pinch.
instance Binary (Enumeration k)

instance Binary a => Binary (Field k a)
