module Network.NSQ
    ( Connection(..)
    , connectNSQD
    , connectNSQLookupD
    , disconnect
    , ErrorMessage
    , MessageHandler
    , Message(..)
    , Packet(..)
    , mpublish
    , publish
    , withConnection
    ) where

import           Control.Concurrent
                 ( ThreadId, forkIO, killThread, threadDelay )
import           Control.Exception           ( Exception(..), throw )
import           Control.Monad               ( forever, void )

import qualified Data.ByteString             as B
import           Data.IORef
                 ( IORef, atomicModifyIORef, atomicWriteIORef, newIORef
                 , readIORef )
import qualified Data.Map.Strict             as M
import           Data.Text                   ( Text, pack, unpack )
import           Data.Text.Encoding          as TSE ( encodeUtf8 )

import           Network.NSQ.Commands
                 ( fin, magic, mpub, nop, pub, rdy, req, sub )
import           Network.NSQ.LookupDResponse
                 ( LookupDResponse(..), LookupException(..)
                 , ProducerResponse(..), address, prAddress, queryLookupD )
import           Network.NSQ.Packet
                 ( Message(..), Packet(..), readPackets )
import           Network.Socket
                 ( AddrInfo(addrAddress, addrFamily), Socket, SocketType(Stream)
                 , close, connect, defaultProtocol, getAddrInfo, socket
                 , withSocketsDo )
import           Network.Socket.ByteString   ( recv, send )

import           System.Random               ( randomRIO )

type ErrorMessage = String

-- | User defined Message handler. A non Nothing return value indicates to requeue the Message.
type MessageHandler = Message -> IO (Maybe ErrorMessage)

-- | Main connection
data Connection = Connection { cnLinks       :: IORef (M.Map Text Link)
                             , cnLookupDAddr :: Maybe Text
                             , cnTopic       :: Text
                             , cnChannel     :: Text
                             , cnHandlers    :: IORef [MessageHandler]
                             }

-- | Single connection running on its own thread
data Link = Link { lnSocket :: Socket
                 , lnBytes  :: B.ByteString
                 , lnId     :: Maybe ThreadId
                 }

connectNSQD :: Text -> Text -> Text -> Text -> IO Connection
connectNSQD topic channel host port = do
    conn <- connect' topic channel host port
    handlers <- newIORef []
    let list = [(address host port, conn)]
        conns = M.fromList list
    links <- newIORef conns
    let c = Connection links Nothing topic channel handlers
    mapM_ (uncurry (startLink c)) list
    pure c

-- | Connect to an nsqlookupd and all nsqd connected to it
connectNSQLookupD :: Text -> Text -> Text -> IO Connection
connectNSQLookupD topic channel url = do
    handlers <- newIORef []
    links <- newIORef mempty
    let c = Connection links (Just url) topic channel handlers
    forkIO $ lookupPoll c url
    pure c

startLink :: Connection -> Text -> Link -> IO ()
startLink c key ln = do tid <- forkIO $ runLink c key ln
                        atomicModifyIORef (cnLinks c) (modifyTid tid)
  where
    modifyTid tid m = (M.insert key (ln { lnId = Just tid }) m, ())

runLink :: Connection -> Text -> Link -> IO ()
runLink c key ln = do
    forever $ do msg <- recv (lnSocket ln) (1024 * 1024)
                 let (packets, bytes) = readPackets (lnBytes ln <> msg)
                 atomicModifyIORef (cnLinks c) (modifyBytes bytes)
                 handlers <- readIORef (cnHandlers c)
                 runCallbacks ln packets handlers
  where
    modifyBytes bytes m = (M.insert key (ln { lnBytes = bytes }) m, ())

runCallbacks :: Link -> [Packet] -> [MessageHandler] -> IO ()
runCallbacks ln packets = mapM_ run
  where
    run handler = mapM_ (handle (lnSocket ln) handler) packets

-- | Thread that polls nsqlookupd for new connections.
lookupPoll :: Connection -> Text -> IO ()
lookupPoll c lookupDAddr = do
    forever $ do res <- queryLookupD lookupDAddr (cnTopic c)
                 either handleError handleSuccess res
                 threadDelay $ 30 * 1000000
  where
    handleSuccess (LookupDResponse producers) = do
        m <- readIORef (cnLinks c)
        let newProducers = filter (not . (`M.member` m) . prAddress) producers
        conns <- mapM (connectProducerResponse (cnTopic c) (cnChannel c))
                      newProducers
        let ls = zip (prAddress <$> newProducers) conns
        mapM_ (uncurry (startLink c)) ls
    handleError err = case fromException err of
        Just TopicNotFoundException -> pure ()
        Nothing -> throw err

-- | Selects a random socket from Connection.
randomSocket :: Connection -> IO Socket
randomSocket c = do list <- M.toList <$> readIORef (cnLinks c)
                    i <- randomRIO (0, length list)
                    pure $ lnSocket $ snd $ list !! i

-- | Publish message to random Link
publish :: Connection -> B.ByteString -> IO Int
publish c bytes = randomSocket c >>= flip send payload
  where
    payload = pub (TSE.encodeUtf8 (cnTopic c)) bytes

-- | Publish messages to random Link
mpublish :: Connection -> [B.ByteString] -> IO Int
mpublish c msgs = randomSocket c >>= flip send payload
  where
    payload = mpub (TSE.encodeUtf8 (cnTopic c)) msgs

-- | Handle packets, providing user handlers the packet if it's a message.
handle :: Socket -> MessageHandler -> Packet -> IO ()
handle sock f p = void $ case p of
    MessagePacket msg  -> f msg >>= handleMsg msg
    ResponsePacket res -> handleResponse res
    ErrorPacket err    -> print err >> send sock nop
  where
    handleMsg msg = send sock
        . maybe (fin (msgId msg)) (const $ req (msgId msg))
    handleResponse _ = send sock nop

-- | Create an internal connection to a single host
connect' :: Text -> Text -> Text -> Text -> IO Link
connect' topic channel host port = do
    withSocketsDo $ do
        serverAddr <- head
            <$> getAddrInfo Nothing (Just (unpack host)) (Just (unpack port))
        sock <- socket (addrFamily serverAddr) Stream defaultProtocol
        connect sock (addrAddress serverAddr)
        mapM_ (send sock) [magic, sub topic' channel', rdy 1]
        pure $ Link sock "" Nothing
  where
    topic' = TSE.encodeUtf8 topic
    channel' = TSE.encodeUtf8 channel

-- | Add a handler to a connection.
withConnection :: Connection -> MessageHandler -> IO ()
withConnection c f = do handlers <- readIORef (cnHandlers c)
                        atomicWriteIORef (cnHandlers c) (f : handlers)

-- | Closes all links in a Connection
disconnect :: Connection -> IO ()
disconnect c = do links <- readIORef (cnLinks c)
                  let lns = snd <$> M.toList links
                  mapM_ closeLink lns

closeLink :: Link -> IO ()
closeLink ln =
    maybe (pure ()) (\tid -> close (lnSocket ln) >> killThread tid) (lnId ln)

connectProducerResponse :: Text -> Text -> ProducerResponse -> IO Link
connectProducerResponse topic channel resp =
    connect' topic
             channel
             (prBroadcastAddress resp)
             (pack (show (prTcpPort resp)))
