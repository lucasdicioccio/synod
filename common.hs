{-# LANGUAGE DeriveGeneric #-}

module Common where

import GHC.Generics (Generic)

-- | A NodeRef is a value to refer to a node participating in the synod.
-- This NodeRef should be unique to any participating node.
data NodeRef = NodeRef Int
    deriving (Show, Eq, Ord, Generic)
