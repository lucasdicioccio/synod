{-# LANGUAGE DeriveGeneric #-}

module Omega where

import Synod
import GHC.Generics (Generic)

data NodeStates = NodeStates {
        leaderRef       :: Maybe NodeRef
    ,   participants    :: [NodeRef]
    } deriving (Show, Ord, Eq, Generic)

type Value' = Either NodeStates

type Acceptor' m v = AcceptFunctions m (Value' v)
type Propose' m v = ProposeFunctions m (Value' v)

-- todo interface functions to:
--  * detect that the leader is missing
--  * propose oneself as the new leader
--  * refuse leader proposals
--
--  for this we need to "hook" ourself to a normal set of AcceptFunctions and encapsulate/decapsulate events when proposing a normal value vs a control-value for omega
--
--
wrapAcceptFunctions :: AcceptFunctions m v -> Acceptor' m v
wrapAcceptFunctions = undefined

wrapProposeFunctions :: ProposeFunctions m v -> Propose' m v
wrapProposeFunctions = undefined

