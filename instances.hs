

module Instances where

import Synod
import Data.Binary

instance Binary NodeRef
instance Binary Decree
instance Binary Proposal
instance Binary Promise
instance Binary Reject
instance (Binary a) => Binary (Accept a)
instance (Binary a) => Binary (AcceptorInput a)

