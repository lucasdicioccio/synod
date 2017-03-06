
module Main where

import Synod
import Instances
import Network.Socket hiding (sendTo, recvFrom)
import Network.Socket.ByteString hiding (getContents)
import System.Environment (getArgs)
import Data.Binary (Binary, encode, decode)
import qualified Data.List as List
import Control.Monad (forever, forM, forM_, liftM, void)
import Control.Concurrent (threadDelay, forkIO, ThreadId)
import Control.Concurrent.Async (race,mapConcurrently)
import Control.Concurrent.STM (atomically, orElse, TChan, newTChanIO, readTChan, writeTChan, TVar, newTVarIO, writeTVar, readTVar, swapTVar, newTVar)
import Control.DeepSeq (deepseq)
import Control.Exception (catch, SomeException)
import qualified Data.ByteString.Lazy.Char8 as C
import qualified Data.ByteString.Char8 as BS
import Data.Maybe (fromMaybe)
import Data.Set (Set)
import qualified Data.Set as Set
import System.Random

type ValueDelta = String
type Value = Round String
type HostString = String
type PortString = String

lbs2bs :: C.ByteString -> BS.ByteString
lbs2bs = BS.pack . C.unpack

bs2lbs :: BS.ByteString -> C.ByteString
bs2lbs = C.pack . BS.unpack

encode' :: Binary a => a -> BS.ByteString
encode' = lbs2bs . encode

decode' :: Binary a => BS.ByteString -> a
decode' = decode . bs2lbs

data AppState = AppState
    { appValue               :: TVar Value
    , appAcceptorTChan       :: TChan (AcceptorInput Value, Peer)
    , appProposerRxTChan     :: TChan (ValueDelta)
    , appProposerTxTChan     :: TChan [(AcceptorInput Value, Peer)]
    , appAcceptorThread      :: ThreadId
    , appProposerThread      :: ThreadId
    , appUdpReceiverThread   :: ThreadId
    , appCommunicationSocket :: Socket
    , appAcceptorPeers       :: [Peer]
    }

-- | Starts an UDP socket listening on a given port.
listenOn :: PortString -> IO (Socket)
listenOn port = do
    putStrLn $ "listening on: " ++ port
    addrinfos <- getAddrInfo (Just (defaultHints {addrFlags = [AI_NUMERICSERV]})) Nothing (Just port)
    let serveraddr = head $ filter (\x -> addrFamily x == AF_INET && addrSocketType x == Datagram) addrinfos
    sock <- socket (addrFamily serveraddr) Datagram defaultProtocol
    bindSocket sock (addrAddress serveraddr)
    return sock

type Peer = (SockAddr, Socket)

-- | Creates a UDP socket to communicate with a peer.
sockPeer :: (HostString,PortString) -> IO (Peer)
sockPeer (ip,port) = do 
    putStrLn $ "connecting to: " ++ show (ip,port)
    addrinfos <- getAddrInfo (Just $ defaultHints { addrFlags = [AI_NUMERICSERV]}) (Just ip) (Just $ port)
    let serveraddr = head $ filter (\x -> addrFamily x == AF_INET && addrSocketType x == Datagram) addrinfos
    sock <- socket (addrFamily serveraddr) Datagram defaultProtocol
    let addr = (addrAddress serveraddr)
    return (addr, sock)

-- | Translates messages received on the socket to messages on channels.
udpReceiver :: Socket -> TChan (AcceptorInput Value, Peer) -> IO ()
udpReceiver sock chan =
        withSocketsDo $ procMessages sock
    where
        procMessages sock = do
             (msg, addr) <- recvFrom sock 1500
             let input = decode' msg :: AcceptorInput Value
             atomically $ writeTChan chan (input, (addr,sock))
             procMessages sock

udpAnswersReceiver
    :: [Peer]
    -> TChan ([Either NoResponse (ProposalResponse Value)])
    -> IO ()
udpAnswersReceiver peers chan = forever $ do
    -- TODO: mechanism to bind answers to sent items
    rsps <- flip mapConcurrently peers $ \(_, sock) -> do
        fmap (Right . decode' . fst) (recvFrom sock 1500)
  
    atomically $ writeTChan chan rsps

-- | Translates messages to send to remote peers.
udpTransmitter
    :: TChan [(AcceptorInput Value, Peer)]
    -> TChan (ProposalResponse Value, Peer)
    -> IO ()
udpTransmitter chan1 chan2 = withSocketsDo $ do
    forever $ do
        next <- atomically $ fmap Left (readTChan chan1) `orElse` fmap Right (readTChan chan2)
        case next of
            Right (rsp, (addr, sock)) -> void $ sendTo sock (encode' rsp) addr
            Left pairs -> void $ mapConcurrently (\(prop, (addr, sock)) -> sendTo sock (encode' prop) addr) pairs

-- | Creates a new application state
initNewState :: FilePath -> PortString -> [(HostString,PortString)] -> IO AppState
initNewState dbPath port peers = do
    acceptorRxTChan <- newTChanIO
    answersTChan    <- newTChanIO
    acceptorTxTChan <- newTChanIO
    proposerRxTChan <- newTChanIO
    proposerTxTChan <- newTChanIO
  
    sock <- listenOn port
    portsMap <- mapM sockPeer peers
  
    val <- ((Just . decode') <$> BS.readFile dbPath) `catch`
            (\y -> seq (y::SomeException) (print ("caught-ignored", y) >> return Nothing))

    valVar <- seq val $ newTVarIO (fromMaybe (Round 0 "") val)

    acceptorThread        <- forkIO $ runAcceptor dbPath valVar acceptorRxTChan acceptorTxTChan
    proposerThread        <- forkIO $ runProposer proposerRxTChan answersTChan proposerTxTChan valVar portsMap
  
    udpReceiverThread     <- forkIO $ udpReceiver sock acceptorRxTChan
    udpAnswersThread      <- forkIO $ udpAnswersReceiver portsMap answersTChan
    udpTransmitterThread  <- forkIO $ udpTransmitter proposerTxTChan acceptorTxTChan
  
    return $ AppState valVar acceptorRxTChan proposerRxTChan proposerTxTChan acceptorThread proposerThread udpReceiverThread sock portsMap

main :: IO ()
main = do
    let format x = let (a,':':b) = List.break (==':') x in (a,b)
    ipPorts <- map format <$> getArgs
    let (_,port) = head ipPorts
    state <- initNewState (port ++ ".db") port ipPorts
    go (appProposerRxTChan state)
  where
    go chan = do
        stream <- getContents
        let vals = lines stream
        forM_ vals $ \val -> atomically $ writeTChan chan val

-- | Runs a proposer.
runProposer :: TChan ValueDelta
            -> TChan [Either NoResponse (ProposalResponse Value)]
            -> TChan [(AcceptorInput Value, Peer)]
            -> TVar Value
            -> [(Peer)]
            -> IO ()
runProposer rxTChan answersTChan txTChan valVar portsMap = do
    refNum <- randomIO
    let ref = NodeRef refNum
    proposer ref (functions portsMap)
  where
    functions peers = ProposerFunctions input
      where
        input = do
            value <- atomically $ do
                str <- readTChan rxTChan
                (Round r rval) <- readTVar valVar
                return (Round (r+1) str)
            putStrLn $ "~ Proposing: " ++ show value
            return (ProposeFunctions sendF waitF sendAcceptF rejectF, value)
          where
            sendF prop = do
                putStrLn $ "~ Sending proposition:" ++ show prop
                let pairs = map (\p -> (prop, p)) peers
                atomically $ writeTChan txTChan pairs

            waitF = do
                putStrLn "~ Waiting for answers:"
                rsps <- atomically $ readTChan answersTChan
                putStrLn $ "~ Answers: " ++ unlines ["  " ++ show r | r <- rsps]
                return rsps

            sendAcceptF acc = do
                putStrLn $ "~ Proposer Accepting the value:" ++ show acc
                let pairs = map (\p -> (acc, p)) peers
                x <- atomically $ do
                    writeTChan txTChan pairs
                    v0 <- readTVar valVar
                    newTVar v0
                return (DistinguishedProposeFunctions (nextDistinguishedValueF x) distinguishedAcceptF)

            distinguishedAcceptF acc = do
                putStrLn $ "~ DistinguishedProposer push the accept:" ++ show acc
                let pairs = map (\p -> (acc, p)) peers
                atomically $ writeTChan txTChan pairs
                -- TODO: get feedback from acceptors or abdicate when rejecting or timing out
                -- for instance we can modify the 'nextVal' function from
                -- DistinguishedProposeFunctions to take an Either Abdicate v
                -- or restart this thread/process

            rejectF val = do
                putStrLn $ "~ Proposition Rejected, decided value is: " ++ show val

            nextDistinguishedValueF mem = do
		 -- Get next value with increasing round number, if a new value
		 -- has been pushed to the acceptor since this time, we need to
		 -- abdicate because this distinguished-accept will fail.
                (v0,v1,v2) <- atomically $ do
                   str <- readTChan rxTChan
                   v1@(Round r rval) <- readTVar valVar
                   let v2 = (Round (r+1) str)
                   v0 <- swapTVar mem v2
                   return $ (v0, v1, v2)
                putStrLn $ "~ DistinguishedProposer next: " ++ show (v0,v1,v2)
                if v0 == v1 then return (Just v2) else putStrLn "~ Abdicating" >> return Nothing

-- | Runs an acceptor.
runAcceptor :: FilePath
            -> TVar Value
            -> TChan (AcceptorInput Value, Peer)
            -> TChan (ProposalResponse Value, Peer)
            -> IO ()
runAcceptor dbPath valVar rxTChan txTChan = do
    x <- (liftM (Just . decode') $ BS.readFile dbPath) `catch`
         (\y -> seq (y::SomeException) (print ("caught-ignored", y) >> return Nothing))
    maybe (emptyAcceptor functions) (\v -> deepseq v (acceptor v functions)) x
  where
    saveState decree value = do
        print ("saving value:", value, "at decree:", decree)
        C.writeFile dbPath (encode (decree, Just $ value))
        atomically $ writeTVar valVar value
    functions = AcceptorFunctions rcv
    rcv = do
        (input, (addr, sock)) <- atomically $ readTChan rxTChan
        let acceptF = AcceptFunctions answer accepted rejected
              where
                answer :: ProposalResponse Value -> IO ()
                answer prop = do
                    putStrLn ("Answering top prop: " ++ show (prop, addr, sock))
                    atomically (writeTChan txTChan (prop, (addr, sock)))

                accepted :: Decree -> Value -> IO ()
                accepted decree value = do
                    putStrLn $ "~ Got value: " ++ show value
                    saveState decree value

                rejected :: Decree -> Value -> IO ()
                rejected decree value =
                    putStrLn $ "~ Rejected value: " ++ show (value, decree, addr, sock)

        return (acceptF, input)
