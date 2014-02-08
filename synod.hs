{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE ExistentialQuantification #-}

-- | Implements the "pure" parts of the single-decree protocol as in "Paxos
-- made simple" paper from Lamport.
module Synod (
        NodeRef (..)
    ,   Decree, Round
    ,   roundNum
    ,   Proposal
    ,   Promise, Reject
    ,   Accept
    -- for the Proposer
    ,   ProposeFunctions (..)
    ,   propose
    ,   ProposerFunctions (..)
    ,   proposer
    -- for the Acceptor
    ,   AcceptorInput
    ,   AcceptFunctions (..)
    ,   accept
    ,   AcceptorFunctions (..)
    ,   acceptor
    ) where

import Data.List (partition, maximumBy)
import Data.Maybe (fromMaybe)
import Data.Either (lefts, rights)
import GHC.Generics (Generic)
import Common

type Round = Int

-- | A Decree is either Zero (i.e., nothing has been decreeted yet) or a decree
-- number along with the NodeRef. The presence of the NodeRef in the Decree is
-- to ensure that no two nodes can issue the same Decree object.
data Decree = Zero Round
    | Decree Round Int NodeRef
    deriving (Show, Eq, Ord, Generic)

roundNum :: Decree -> Round
roundNum (Zero r)       = r
roundNum (Decree r _ _) = r

-- | When iteratively proposing new values, you just need to track the decree
-- that you should use in your proposal to have some chance of success.
type ProposerState = (NodeRef, Decree)

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
      promiseNumber   :: Decree
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
    ,   waitAnswers     :: m [ProposalResponse v]
    ,   sendAccept      :: AcceptorInput v -> m ()
    ,   reject          :: Maybe v -> m ()
    }

-- | Interface functions required to run a new Proposer loop.
data ProposerFunctions m v = ProposerFunctions {
        proposeValue :: m (ProposeFunctions m v, v)
    }

-- | Interface functions that an Acceptor must implment for a single round.
data AcceptFunctions m v = AcceptFunctions {
      answerProposition :: ProposalResponse v -> m ()
    , putCurrentValue   :: Decree -> v -> m ()
    , getCurrentValue   :: Decree -> m (Maybe v)
    }

-- | Interface functions required to run a new Acceptor loop.
--
-- Basically, you need a function that reacts to an event and returns an
-- AcceptorInput representation of the event you react to, as well as functions
-- to run one round of the acceptor in whatever is your Monad.
data AcceptorFunctions m v = AcceptorFunctions {
      receiveMessage    :: m (AcceptFunctions m v, AcceptorInput v)
    }

{-
 - HELPER FUNCTIONS
 -}

-- | Increments the Decree value and substitutes the NodeRef for the "lower
-- bits" of the comparison.
incrementDecree :: NodeRef -> Decree -> Decree
incrementDecree ref (Zero r)        = Decree r 0 ref
incrementDecree ref (Decree r n _)  = Decree r (n + 1) ref

-- | Increments the Decree's round value
incrementRound :: Decree -> Decree
incrementRound (Zero r)          = Zero (r+1)
incrementRound (Decree r n ref)  = Decree (r+1) n ref

{-
 - PROPOSER FUNCTIONS
 -}

-- | Runs one round of Proposer.
propose :: (Monad m) => ProposerState         -- ^ state of the proposer
                     -> ProposeFunctions m v  -- ^ functions to transmit propositions/accepted value
                     -> v                     -- ^ the value that we try to propose, lazyness is a virtue here because if the value is never used, no-need to compute the value
                     -> m (Bool, Decree)
propose (ref, num) comm proposedVal = do

    sendProposition comm $ InProp $ Proposal (incrementDecree ref num)
    responses <- waitAnswers comm

    let (success, maxNum, val) = decide (num, proposedVal) responses

    if success
    then sendAccept comm (InAccept $ Accept (fromMaybe proposedVal val) maxNum)
    else reject comm $ val

    return (success, maxNum)

decide :: (Decree, v) -> [ProposalResponse v] -> AcceptDecision v
decide (num, val) rsps = 
    let (rejects, promises) = (lefts rsps, rights rsps) in
    let success = length promises > length rejects in
    let answers = map (either rejectVal promiseVal) rsps in
    let pairs = (num, Just val) : answers in
    let f (num1, _) (num2, _) = num1 `compare` num2 in
    let (maxDecree, maxVal) = maximumBy f pairs in

    (success, maxDecree, maxVal)

-- | Proposer Loop
proposer :: (Monad m) => NodeRef -> ProposerFunctions m v -> m ()
proposer ref comm = go (ref, Zero 0)
    where go st0 = do
            (functions, value) <- proposeValue comm
            (success, maxNum) <- propose st0 functions value
            if success
            then go (ref, incrementRound maxNum)
            else go (ref, maxNum)

{-
 - ACCEPTOR FUNCTIONS
 -}

-- | Runs one round of Acceptor.
accept :: (Monad m) => AcceptorState v      -- ^ current state of the acceptor
                    -> AcceptFunctions m v  -- ^ functions to transmit responses/communicate accepted values
                    -> AcceptorInput v      -- ^ input event we are reacting-to
                    -> m (AcceptorState v)
accept state comm (InAccept (Accept val num)) = handleAcceptInput state comm num val
accept state comm (InProp prop)               = handlePropositionInput state comm prop

-- | Handle case where acceptor received a value agreed upon.
handleAcceptInput state comm num val = do
    putCurrentValue comm num val
    return $ state { latestAccepted = num }

-- | Handle case where acceptor received a propoition for a new value
handlePropositionInput :: Monad m => AcceptorState v
                                  -> AcceptFunctions m v
                                  -> Proposal
                                  -> m (AcceptorState v)
handlePropositionInput state comm prop = do
    val <- getCurrentValue comm $ proposalNumber prop
    let newVal = decideOnProposal prop state
    let rsp = maybe newVal rejectExisting val
    answerProposition comm rsp
    return $ either (const state) (\prom -> state { highestPromise = promiseNumber prom}) rsp

    where rejectExisting val = Left $ Reject (proposalNumber prop) (latestAccepted state, Just val)
    

-- | Decides whether we should accept or reject the proposal.
decideOnProposal :: Proposal -> AcceptorState v -> ProposalResponse v
decideOnProposal (Proposal dProp) state 
    | shouldAccept dProp state      = Right $ answer Promise dProp (state { highestPromise = dProp})
    | otherwise                     = Left $ answer Reject dProp state

-- where shouldAccept prop state = (highestPromise state == Zero) ||
    where shouldAccept prop state = prop > highestPromise state
          answer f prop state = f prop (latestAccepted state, currentValue state)

-- | Acceptor Loop
acceptor :: (Monad m) => AcceptorFunctions m v -> m ()
acceptor comm = go st0
    where st0 = AcceptorState (Zero 0) (Zero 0) Nothing
          go state = do
                (functions, input) <- receiveMessage comm
                accept state functions input >>= go
