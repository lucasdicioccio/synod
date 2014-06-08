{-# LANGUAGE DeriveFunctor #-}
module Synod.Node where

import Synod.SingleDecree
import Data.IntMap (IntMap)
import Control.Monad.Free

type Tst = Int

data Op v next = RunForElections next
  | AddPeer NodeRef next
  | RemovePeer NodeRef next
  | LogValue v next
  deriving (Functor)

type OpM a = Free (Op a)

toto = do
  x <- register 42

eval :: (Monad m) => Node v -> OpM v a -> m ()
eval n (Pure _)                 = return ()
eval n (Free (LogValue v f))    = do
  let n' = n { nodeEventLog = undefined }
  eval n' f

data Node a = Node
  { nodeRef         :: Int                        -- reference of this node
  , nodePeers       :: [(Tst, NodeRef)]     -- known participants
  , nodeLeader      :: Maybe (Tst, NodeRef) -- current known leader
  , nodeEventLog    :: IntMap a                   -- in-memory log of committed actions
  , nodePendingLog  :: IntMap a                   -- in-memory log of to-be confirmed actions
  }
