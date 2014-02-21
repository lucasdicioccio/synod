
module System where

import Synod
import System.Random
import Control.Concurrent
import Control.Concurrent.Async
import Control.Monad.IO.Class
import Data.Maybe

-- Function to monitor a leader as a simple follower.
-- maybe we should also provide a way to register on a leader death
--
-- todo: explore with parametrized monad rather IO; IO is convenient because we
-- can use it in race without learning requiring monad transformation (e.g.,
-- for Control.Concurrent.Async.race function)
newtype LeaderMonitor = LeaderMonitor { runMonitorFunction :: IO (Bool) }

instance Show LeaderMonitor where
    show _ = "LeaderMonitor { runMonitorFunction :: IO () }"

data NodeState = NodeState {
        nodeRef             :: NodeRef
    ,   knownPeers          :: [NodeRef]
    ,   knownLeader         :: Maybe (NodeRef, LeaderMonitor)
    ,   lastKnownInstance   :: InstanceNumber
    ,   delays              :: (Int, Int, Int, Int) -- min leader claim / max leader claim / leader push / eligible down
    } deriving (Show)

newNode :: NodeRef -> NodeState
newNode ref = NodeState ref [] Nothing zerothInstance (1000, 1000000, 2500000, 5000000)

{-
type SharedState = MVar NodeState

newShareState :: NodeRef -> IO (SharedState)
newShareState = newMVar . newNode

runNode :: (MonadIO m) => NodeRef -> m ()
runNode node = do
    sharedState <- liftIO $ newShareState node
    return ()
-}

findNewLeader :: (MonadIO m) => NodeState -> m (Maybe (NodeRef, LeaderMonitor))
findNewLeader st@NodeState{knownLeader = lead, delays = (dMin,dMax,_,heartbeat)}
    | isNothing lead = liftIO (randomRIO (dMin, dMax) >>= threadDelay) >> claimLeadership st
    | otherwise      = liftIO (race (threadDelay heartbeat) (expectLeaderLifeProof st))
                       >>= either (const $ claimLeadership st)
                                  (ifElse (return lead) (claimLeadership st))

ifElse a b True = a
ifElse a b _    = b

-- if there is no way to show that there is a leader, then it is like if there
-- is no leader
expectLeaderLifeProof :: NodeState -> IO (Bool)
expectLeaderLifeProof st@NodeState{knownLeader = lead} =
    case lead of
        Nothing         -> return False
        Just (_, f)     -> runMonitorFunction f

claimLeadership :: (MonadIO m) => NodeState -> m (Maybe (NodeRef, LeaderMonitor))
claimLeadership st = do
    toto <- sendClaimProposition comm (nodeRef st)
    return Nothing
