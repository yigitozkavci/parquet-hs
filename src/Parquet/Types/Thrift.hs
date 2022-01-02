{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE UndecidableInstances #-}

-- |
module Parquet.Types.Thrift
  ( -- *
    pinchField,
    pinchPos,

    -- *
    unField
  )
where

------------------------------------------------------------------------------

import Control.Lens
import qualified Data.Generics.Product.Fields as GL
import qualified Data.Generics.Product.Positions as GL
import GHC.Generics (D, M1, Meta (..), Rep)
import GHC.TypeLits (AppendSymbol, Symbol)
import Parquet.Prelude
import Pinch

------------------------------------------------------------------------------

-- |
type family TypeName a :: Symbol where
  TypeName (M1 D ('MetaData name _ _ _) f ()) = name
  TypeName a = TypeName (Rep a ())

------------------------------------------------------------------------------

-- |
pinchPos ::
  forall pos s t a1 b1 a2 b2.
  (GL.HasPosition 1 a1 b1 a2 b2, GL.HasPosition pos s t a1 b1) =>
  Lens s t a2 b2
pinchPos = GL.position @pos . GL.position @1

------------------------------------------------------------------------------

-- |
pinchField ::
  forall field s i r field_name.
  ( field_name ~ ("_" `AppendSymbol` TypeName s `AppendSymbol` "_" `AppendSymbol` field),
    GL.HasPosition 1 i i r r,
    GL.HasField field_name s s i i
  ) =>
  Lens s s r r
pinchField = GL.field @field_name . GL.position @1

------------------------------------------------------------------------------

-- |
unField :: Field n a -> a
unField (Field a) = a
