{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module Instances where

import Debug.Trace

import Synod
import Data.Binary
import Data.List (tails)
import Control.DeepSeq
import Data.Set
import GHC.Generics

instance Binary NodeRef
instance Binary Decree
instance Binary Proposal
instance Binary NoResponse
instance (Binary v) => Binary (Promise v)
instance (Binary v) => Binary (Reject v)
instance (Binary a) => Binary (Accept a)
instance (Binary a) => Binary (AcceptorInput a)
instance NFData NodeRef
instance NFData Decree
instance NFData NoResponse

-- | Integer grow monotonically.
instance Grow Int where
  grow a b = a
  precedes old new = old > new

-- | Lists are free monoids. Never lose information.
instance Eq v => Grow [v] where
  grow xs ys = xs++ys
  precedes old new = old `elem` tails new

-- | Sets can't have dupes.
instance Ord v => Grow (Set v) where
  grow xs ys = xs `union` ys
  precedes old new = old `isSubsetOf` new

instance (Grow a, Grow b) => Grow (a, b) where
  grow (a1, b1) (a0, b0) = (grow a1 a0, grow b1 b0)
  precedes (a0, b0) (a1, b1) = precedes a0 a1 && precedes b0 b1
    -- TODO: partial/biased order

-- | Monotonically increasing values can have a round of updates.
data Sample a = Sample { sampleNumber :: Int , sampleVal :: a}
  deriving (Show,Ord,Eq,Generic)

instance Grow (Sample v) where
  grow     new          _    = new
  precedes oldRound newRound = sampleNumber oldRound < sampleNumber newRound

instance (Binary a) => Binary (Sample a)
instance (NFData a) => NFData (Sample a)

-- | Monotonically increasing values tolerating no "holes" during an update.
data Round a = Round { roundNumber :: Int , roundVal :: a}
  deriving (Show,Ord,Eq,Generic)

instance Grow (Round v) where
  grow     new          _    = new
  precedes oldRound newRound = roundNumber oldRound + 1 == roundNumber newRound

instance (Binary a) => Binary (Round a)
instance (NFData a) => NFData (Round a)
