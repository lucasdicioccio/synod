
module MRefs where

import Synod
import Instances
import Network.Socket hiding (send, sendTo, recv, recvFrom)
import Network.Socket.ByteString hiding (getContents)
import Network.BSD
import System.Environment
import Data.Binary (Binary, encode, decode)
import qualified Data.Map as M
import Control.Monad
import Control.Concurrent
import Control.Concurrent.Async
import Control.Concurrent.STM
import Data.IORef
import qualified Data.ByteString.Lazy.Char8 as C
import qualified Data.ByteString.Char8 as BS

type Value = String

lbs2bs :: C.ByteString -> BS.ByteString
lbs2bs = BS.pack . C.unpack

bs2lbs :: BS.ByteString -> C.ByteString
bs2lbs = C.pack . BS.unpack

encode' :: Binary a => a -> BS.ByteString
encode' = lbs2bs . encode

decode' :: Binary a => BS.ByteString -> a
decode' = decode . bs2lbs

main = do
    remotes@(acceptorPort:_) <- getArgs
    acceptorChan <- newChan :: IO (Chan (AcceptorInput Value, Socket, SockAddr))
    proposerChan <- newChan :: IO (Chan Value)
    forkIO $ listenForCommands acceptorPort acceptorChan
    forkIO $ runAcceptor acceptorChan
    forkIO $ runProposer acceptorPort proposerChan remotes
    go proposerChan
    where go chan = do
            stream <- getContents
            let vals = lines stream
            forM_ vals $ \val -> do
               writeChan chan val

listenForCommands port chan = withSocketsDo $ do
    addrinfos <- getAddrInfo 
                    (Just (defaultHints {addrFlags = [AI_PASSIVE]}))
                    Nothing (Just port)
    let serveraddr = head addrinfos
    sock <- socket (addrFamily serveraddr) Datagram defaultProtocol
    bindSocket sock (addrAddress serveraddr)
    procMessages sock
        where procMessages sock = do
                                (msg, addr) <- recvFrom sock 1500
                                let input = decode' msg :: AcceptorInput Value
                                writeChan chan (input,sock,addr)
                                procMessages sock

runProposer :: String -> Chan Value -> [ServiceName] -> IO ()
runProposer port chan peers = do
    let ref = NodeRef (read port)
    portsMap <- mapM sockPeer peers
    proposer ref (functions portsMap)
    where functions pairs = ProposerFunctions input
            where input = do
                    value <- readChan chan
                    print $ "Proposing: " ++ value
                    return (ProposeFunctions sendF waitF pushF rejF, value)
                    where sendF prop = do
                            print "Sending proposition:"
                            print prop
                            forM_ pairs $ \(addr, sock) -> do
                                let msg = (encode' prop)
                                sendTo sock msg addr
                          waitF = do
                            print "Waiting for answers:"
                            rsps <- forM pairs $ \(_, sock) -> do
                                (msg,addr) <- recvFrom sock 1500
                                return $ decode' msg
                            print rsps
                            return rsps
                          pushF acc = do
                            print "Accepting the value:"
                            print acc
                            forM_ pairs $ \(addr, sock) -> do
                                sendTo sock (encode' acc) addr
                          rejF current = do
                            print $ "rejected, current is: " ++ show current

sockPeer ::  ServiceName -> IO (SockAddr, Socket)
sockPeer peer = do 
            addrinfos <- getAddrInfo Nothing (Just "localhost") (Just peer)
            let serveraddr = head addrinfos
            sock <- socket (addrFamily serveraddr) Datagram defaultProtocol
            let addr = (addrAddress serveraddr)
            return (addr, sock)

runAcceptor ::  Chan (AcceptorInput Value, Socket, SockAddr) -> IO ()
runAcceptor chan = do
    acceptor functions
    where functions = AcceptorFunctions rcv
          rcv = do
            (input, sock, addr) <- readChan chan
            let acceptF = AcceptFunctions answer communicate
                            where communicate :: Decree -> Value -> IO ()
                                  communicate decree value = print "got value" >> print value
                                  answer prop = print "answering" >> print prop >> void (sendTo sock (encode' prop) addr)
            return (acceptF, input)
