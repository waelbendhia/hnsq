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

import Control.Concurrent
import Control.Monad

import qualified Data.ByteString as B
import qualified Data.Map.Strict as M

import Data.Text.Encoding as TSE
import Data.Text (Text, pack, unpack)

import Data.IORef

import Network.Socket hiding (send, sendTo, recv, recvFrom)
import Network.Socket.ByteString (send, recv)

import Network.NSQ.LookupDResponse (LookupDResponse(..), ProducerResponse(..), address, prAddress, queryLookupD)
import Network.NSQ.Packet (Message(..), Packet(..), readPackets)
import Network.NSQ.Commands

import System.Random (randomRIO)

type ErrorMessage = String

-- | User defined Message handler. A non Nothing return value indicates to requeue the Message.
type MessageHandler = Message -> IO (Maybe ErrorMessage)

-- | Main connection
data Connection = Connection
  { cnLinks :: IORef (M.Map Text Link)
  , cnLookupDAddr :: Maybe Text
  , cnTopic :: Text
  , cnChannel :: Text
  , cnHandlers :: IORef [MessageHandler]
  }

-- | Single connection running on its own thread
data Link = Link
  { lnSocket :: Socket
  , lnBytes :: B.ByteString
  , lnId :: Maybe ThreadId
  }

connectNSQD :: Text -> Text -> Text -> Text -> IO Connection
connectNSQD topic channel host port = do
  conn <- connect' topic channel host port
  bytes <- newIORef ""
  handlers <- newIORef []
  let list = [(address host port, conn)]
      conns = M.fromList list
  links <- newIORef conns
  let c = Connection links Nothing topic channel handlers
  mapM_ (run c) list
  return c
  where
    run c (key, conn) = startLink c key conn

-- | Connect to an nsqlookupd and all nsqd connected to it
connectNSQLookupD :: Text -> Text -> Text -> IO Connection
connectNSQLookupD topic channel url = do
  res <- queryLookupD url topic
  case res of
    Left err -> error err
    Right (LookupDResponse xs) -> do
      handlers <- newIORef []
      let addrs = fmap prAddress xs
      conns <- mapM (connectProducerResponse topic channel) xs
      let list = zip addrs conns
      links <- newIORef $ M.fromList list
      let c = Connection links (Just url) topic channel handlers
      forkIO $ lookupPoll c
      mapM_ (run c) list
      return c
      where
        run c (key, conn) = startLink c key conn

startLink :: Connection -> Text -> Link -> IO ()
startLink c@Connection{..} key ln = do
  tid <- forkIO $ runLink c key ln 
  atomicModifyIORef cnLinks (modifyTid tid)
  where
    modifyTid tid m = (M.insert key (ln { lnId = Just tid }) m, ())

runLink :: Connection -> Text -> Link -> IO ()
runLink c@Connection{..} key ln@Link{..} = do
  forever $ do
    msg <- recv lnSocket 4096
    let bytes = lnBytes <> msg
        (packets, bytes') = readPackets bytes
    atomicModifyIORef cnLinks (modifyBytes bytes')
    readIORef cnHandlers >>= runCallbacks ln packets
    where
      modifyBytes bytes m = (M.insert key (ln { lnBytes = bytes }) m, ())

runCallbacks :: Link -> [Packet] -> [MessageHandler] -> IO ()
runCallbacks Link{..} packets handlers =
  forM_ handlers run
  where
    run handler = mapM_ (handle lnSocket handler) packets

-- | Thread that polls nsqlookupd for new connections.
lookupPoll :: Connection -> IO ()
lookupPoll Connection{..} = do
  case cnLookupDAddr of
    Nothing -> return ()
    Just url -> forever $ do
      res <- queryLookupD url cnTopic
      case res of 
        Left err -> error err
        Right (LookupDResponse producers) -> do 
          m <- readIORef cnLinks
          let newProducers = filter (notInMap m) producers
          conns <- mapM (connectProducerResponse cnTopic cnChannel) newProducers
          let newMap = M.fromList $ zip (fmap prAddress producers) conns
          atomicWriteIORef cnLinks (M.union m newMap)
          where
            notInMap mp pr = null . M.lookup (prAddress pr) $ mp
      threadDelay $ 30 * 1000000

-- | Selects a random socket from Connection.
randomSocket :: Connection -> IO Socket
randomSocket Connection{..} = do
    list <- fmap M.toList $ readIORef cnLinks
    i <- randomRIO (0, length list)
    return $ lnSocket . snd $ list !! i

-- | Publish message to random Link
publish :: Connection -> B.ByteString -> IO Int
publish c@Connection{..} bytes =
  randomSocket c >>= flip send payload
  where
    payload = pub (TSE.encodeUtf8 cnTopic) bytes

-- | Publish messages to random Link
mpublish :: Connection -> [B.ByteString] -> IO Int
mpublish c@Connection{..} msgs =
  randomSocket c >>= flip send payload
  where
    payload = mpub (TSE.encodeUtf8 cnTopic) msgs

-- | Handle packets, providing user handlers the packet if it's a message.
handle :: Socket -> MessageHandler -> Packet -> IO ()
handle sock f p = do
  case p of
    MessagePacket msg -> f msg >>= handleMsg msg
    ResponsePacket res -> handleResponse res
    ErrorPacket err -> print err >> send sock nop
  return ()
  where
    handleMsg Message{..} res = case res of
                      Just err -> send sock $ req msgId
                      Nothing -> send sock $ fin msgId
    handleResponse _ = send sock nop

-- | Create an internal connection to a single host
connect' :: Text -> Text -> Text -> Text -> IO Link
connect' topic channel host port = do
  withSocketsDo $ do
    serverAddr <- fmap head $ getAddrInfo Nothing (Just (unpack host)) (Just (unpack port))
    sock <- socket (addrFamily serverAddr) Stream defaultProtocol
    connect sock (addrAddress serverAddr)
    send sock magic
    send sock (sub topic' channel')
    send sock $ rdy 1
    return $ Link sock "" Nothing
    where
      topic' = TSE.encodeUtf8 topic
      channel' = TSE.encodeUtf8 channel

-- | Add a handler to a connection.
withConnection :: Connection -> MessageHandler -> IO ()
withConnection Connection{..} f = do
  handlers <- readIORef cnHandlers
  atomicWriteIORef cnHandlers (f:handlers)

-- | Closes all links in a Connection
disconnect :: Connection -> IO ()
disconnect Connection{..} = do
  links <- readIORef cnLinks
  let lns = fmap snd $ M.toList links
  mapM_ closeLink lns
  return ()

closeLink :: Link -> IO ()
closeLink Link{..} =
  case lnId of 
    Nothing -> return ()
    Just tid -> close lnSocket >> killThread tid

connectProducerResponse :: Text -> Text -> ProducerResponse -> IO Link
connectProducerResponse topic channel ProducerResponse{..} =
  connect' topic channel prBroadcastAddress (pack (show prTcpPort))
