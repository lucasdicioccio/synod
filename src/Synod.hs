{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE RecordWildCards #-}

-- | Implements the "pure" parts of the single-decree protocol as in "Paxos
-- made simple" paper from Lamport.
module Synod (
        NodeRef (..)
    ,   Decree
    ,   Proposal
    ,   Promise, Reject
    ,   Accept
    -- for the Proposer
    ,   ProposeFunctions (..)
    ,   propose
    ,   ProposerFunctions (..)
    ,   proposer
    ,   DistinguishedProposeFunctions (..)
    ,   ProposalResponse
    -- for the Acceptor
    ,   AcceptorInput
    ,   AcceptFunctions (..)
    ,   accept
    ,   AcceptorFunctions (..)
    ,   emptyAcceptor
    ,   acceptor
    ,   NoResponse (..)
    -- typeclass for decided-on data
    ,   Grow (..)
    ) where

import Data.List (partition, maximumBy)
import Data.Maybe (fromMaybe, isJust)
import Data.Either (lefts, rights)
import GHC.Generics (Generic)
import Control.Monad (void)

-- | A NodeRef is a value to refer to a node participating in the synod.
-- This NodeRef should be unique to any participating node.
data NodeRef = NodeRef Int
    deriving (Show, Eq, Ord, Generic)

-- | A Decree is either Zero (i.e., nothing has been decreeted yet) or a decree
-- number along with the NodeRef. The presence of the NodeRef in the Decree is
-- to ensure that no two nodes can issue the same Decree object.
data Decree = Zero | Decree Int NodeRef
    deriving (Show, Eq, Ord, Generic)

-- | When iteratively proposing new values, you just need to track the decree
-- that you should use in your proposal to have some chance of success.
type ProposerState = (NodeRef, Decree)

type DistinguishedProposerState v = (NodeRef, Decree, v)

-- | A Decree proposal. The value associated to the decree gets sent only after
-- the acceptors accept the decree. Hence there is no "value" field in this record.
data Proposal = Proposal {
      proposalNumber  :: Decree
    } deriving (Show, Generic) 

-- | A Promise that an acceptor should return when accepting a Proposal.
--
-- When a node issues a Promise, it must specify two things: on one hand, the
-- Decree number it is answering to, on the other hand, the highest Decree
-- number that this answering node *accepted* so far.
data Promise v = Promise {
      promiseNumber :: Decree
    , promiseVal :: (Decree, Maybe v)
    } deriving (Show, Generic)

-- | A Reject that an acceptor can return to refuse a proposal.
--
-- Instead of accepting a proposal, a node might refuse it (e.g., because it
-- already made a promise for an higher decree to someone else).
-- Thus, when rejecting, you need two pieces of information: on one hand, to
-- match the proposal with the rejection, one needs the Decree number that is
-- being rejected. On the the other hand, when rejecting, a decree, you can
-- give a chance to the issuer to update its decree number for the next time.
-- Thus this object also carries the highest Decree value that was *accepted*
-- by the rejecting acceptor.
data Reject v = Reject {
      rejectNumber          :: Decree
    , rejectVal             :: (Decree, Maybe v)
    } deriving (Show, Generic)

data NoResponse = NoResponse deriving (Show, Generic)

-- | A response to a proposal is either a reject, or a promise to reject
-- decrees smaller than the proposed decree.
type ProposalResponse v = Either (Reject v) (Promise v)

-- | When deciding what to do with a set of ProposalResponse, a proposer need
-- to decide a Decree number. We call that an AcceptDecision (either should
-- reject or should accept the value.
--
-- At the same time, we need to select a decree number to accept.
type AcceptDecision v = (Bool, Decree, Maybe v)

-- | Once a decree proposal has been answered by a majority of acceptors, the issuer
-- decide whether or not to accept the decree. In the eventuality where the
-- decree is accepted, the issuer need to give the value for the decree. 
--
-- Note that the commited decree number may be different than the initial
-- Proposal value. However the acceptors answered with a promise not to accept
-- decrees smaller than a given value. Thus, for this Accept to eventually get
-- accepted by a majority of acceptor, the decree in this Accept must be no
-- lower than the highest in the promises.
data Accept v = Accept {
      value :: v
    , commitNumber :: Decree
    } deriving (Show, Generic)

-- | Current state of an acceptor. You need a current value v (if any).  In
-- addition, the acceptor must know what is the last Decree number associated
-- to the value, as well as the Decree number of the highest promise it made to
-- a proposer.
data AcceptorState v = AcceptorState {
      highestPromise :: Decree
    , latestAccepted :: Decree
    , currentValue :: Maybe v
    } deriving (Show)


-- | The input messages that an Acceptor may receive are of two types: a
-- proposal or an accept.
data AcceptorInput val = InProp Proposal | InAccept (Accept val)
    deriving (Show,Generic)

-- | Interface functions that a Proposer must implement for a single round.
data ProposeFunctions m v = ProposeFunctions {
        sendProposition :: AcceptorInput v -> m ()
    ,   waitAnswers     :: m [Either NoResponse (ProposalResponse v)]
    ,   sendAccept      :: AcceptorInput v -> m (DistinguishedProposeFunctions m v)
    ,   reject          :: Maybe v -> m ()
    }

-- | Interface functions that a DistinguishedProposer must implement for a stream of rounds.
data DistinguishedProposeFunctions m v = DistinguishedProposeFunctions {
        nextValue :: m v
    ,   distinguishedSendAccept :: AcceptorInput v -> m ()
    }

-- | Interface functions required to run a new Proposer loop.
data ProposerFunctions m v = ProposerFunctions {
        proposeValue :: m (ProposeFunctions m v, v)
    }

-- | Interface functions that an Acceptor must implment for a single round.
data AcceptFunctions m v = AcceptFunctions {
      answerProposition         :: ProposalResponse v -> m ()
    , communicateValue          :: Decree -> v -> m ()
    , communicateRejectedValue  :: Decree -> v -> m ()
    }

-- | Interface functions required to run a new Acceptor loop.
--
-- Basically, you need a function that reacts to an event and returns an
-- AcceptorInput representation of the event you react to, as well as functions
-- to run one round of the acceptor in whatever is your Monad.
data AcceptorFunctions m v = AcceptorFunctions {
      receiveMessage    :: m (AcceptFunctions m v, AcceptorInput v)
    }

-- | Class of growable values.
-- TODO: add delta vs total
--       delta used in the proposer and the decision
--       grow used in the acceptors to collect the actual stuff
class Grow v where
  grow :: v -> v -> v
  precedes :: v -> v -> Bool

{-
 - HELPER FUNCTIONS
 -}

-- | Increments the Decree value and substitutes the NodeRef for the "lower
-- bits" of the comparison.
increment :: NodeRef -> Decree -> Decree
increment ref Zero          = Decree 0 ref
increment ref (Decree n _)  = Decree (n + 1) ref

{-
 - PROPOSER FUNCTIONS
 -}

-- | Runs one round of Proposer.
propose :: (Grow v, Show v, Monad m) => ProposerState         -- ^ state of the proposer
                     -> ProposeFunctions m v  -- ^ functions to transmit propositions/accepted value
                     -> v                     -- ^ the value that we try to propose, lazyness is a virtue here because if the value is never used, no-need to compute the value
                     -> m ProposerState
propose (ref, num) (comm@ProposeFunctions{..}) proposedVal = do

    sendProposition $ InProp $ Proposal (increment ref num)
    responses <- waitAnswers

    let (majority, maxNum, val) = decide responses
    let newMaxNum = increment ref maxNum

    -- Given a majority of accepts, we can send an accept and step up as
    -- distinguished proposer.
    -- Otherwise, we may either have a value from the set of acceptors
    -- or no informaion at all (e.g., when all messages timed out).
    -- If we have a best known value, we can broadcast it around so
    -- that acceptors that are lagging behind can catch up.
    if majority
    then do
        let newVal = maybe proposedVal (grow proposedVal) val
        functions <- sendAccept (InAccept $ Accept newVal newMaxNum)
        distinguishedPropose (ref,newMaxNum,newVal) functions
    else do
        reject val
        -- repair old acceptors, currently broadcasted
        -- TODO: cherry-pick repaired acceptor instead of broadcast
        maybe (return ()) (\x -> void $ sendAccept (InAccept $ Accept x newMaxNum)) val
        return (ref, newMaxNum)

distinguishedPropose :: (Grow v, Monad m) =>
  (NodeRef,Decree,v) -> DistinguishedProposeFunctions m v -> m ProposerState
distinguishedPropose (ref,num,val) (comm@DistinguishedProposeFunctions{..}) = do
    nextVal <- nextValue
    let newVal = grow nextVal val
    let newNum = increment ref num
    distinguishedSendAccept (InAccept $ Accept newVal newNum)
    distinguishedPropose (ref,newNum,newVal) comm

decide :: (Show v) => [Either NoResponse (ProposalResponse v)] -> AcceptDecision v
decide results = 
    let drops = lefts results
        rsps = rights results
        rejects = lefts rsps
        promises = rights rsps
        majority = length promises > length rejects + length drops
        answers = map (either rejectVal promiseVal) rsps
        f (num1, _) (num2, _) = num1 `compare` num2
        (maxNum, maxVal) = maximumBy f answers
    in (majority, maxNum, maxVal)

-- | Proposer Loop
-- proposer :: (Monad m) => NodeRef -> ProposerFunctions m v -> m ()
proposer ref (comm@ProposerFunctions{..}) = go (ref, Zero)
    where go state = do
            putStrLn $ "** proposer " ++ show state
            (functions, value) <- proposeValue
            propose state functions value >>= go

{-
 - ACCEPTOR FUNCTIONS
 -}

-- | Runs one round of Acceptor.
accept :: (Monad m, Grow v) => AcceptorState v
                            -- ^ current state of the acceptor
                            -> AcceptFunctions m v 
                            -- ^ functions to transmit responses/communicate accepted values
                            -> AcceptorInput v
                            -- ^ input event we are reacting-to
                            -> m (AcceptorState v)
accept state comm (InAccept (Accept val num)) = handleAcceptInput state comm num val
accept state comm (InProp prop)               = handlePropositionInput state comm prop

handleAcceptInput state (comm@AcceptFunctions{..}) num val 
  -- We may have promised to never accept values larger than a given value
  -- hence check that the current one works.
  | (num >= highestPromise state) &&
    (maybe True (\x -> x `precedes` val) (currentValue state)) = do
        communicateValue num val
        return $ state { highestPromise = num
                       , latestAccepted = num
                       , currentValue   = Just val
                       }
  | otherwise = do
        communicateRejectedValue num val
        return state

handlePropositionInput state (comm@AcceptFunctions{..}) prop = do
    let (proposed, answer) = handleProposal prop state
    answerProposition answer
    return $ maybe state (\dec -> state { highestPromise = dec }) proposed

handleProposal :: Proposal -> AcceptorState v -> (Maybe Decree, ProposalResponse v)
handleProposal (Proposal dProp) acceptor 
    | dProp > highestPromise acceptor = (Just $ dProp, Right $ answer Promise dProp (acceptor { highestPromise = dProp}))
    | otherwise                       = (Nothing, Left $ answer Reject dProp acceptor)
    where answer f prop acceptor = f prop ( latestAccepted acceptor 
                                          , currentValue acceptor)

-- | Acceptor Loop
--acceptor :: (Monad m) => AcceptorFunctions m v -> m ()
emptyAcceptor comm = acceptor (Zero, Nothing) comm

acceptor (x,val) comm = go (AcceptorState x x val)
        where go state = do
                putStrLn $ "** acceptor " ++ show state
                (functions, input) <- receiveMessage comm
                accept state functions input >>= go
