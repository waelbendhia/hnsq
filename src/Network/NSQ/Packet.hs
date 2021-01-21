module Network.NSQ.Packet ( Packet(..), Message(..), readPackets ) where

import           Data.Binary.Get
import qualified Data.ByteString      as B
import qualified Data.ByteString.Lazy as BL
import           Data.Int

data Packet = MessagePacket Message
            | ErrorPacket B.ByteString
            | ResponsePacket B.ByteString
    deriving ( Show )

data Message = Message { msgTimestamp :: Int64
                       , msgAttempts  :: Int16
                       , msgId        :: B.ByteString
                       , msgBytes     :: B.ByteString
                       }
    deriving ( Show )

-- returns list of packets and remaining unprocessed bytes, if any.
readPackets :: B.ByteString -> ([Packet], B.ByteString)
readPackets bytes = case readPacket bytes of
    (Nothing, _)       -> ([], bytes)
    (Just msg, bytes') ->
        let (next, bytes'') = readPackets bytes' in (msg : next, bytes'')

readPacket :: B.ByteString -> (Maybe Packet, B.ByteString)
readPacket bytes = case runGetOrFail get (BL.fromStrict bytes) of
    Left _ -> (Nothing, bytes)
    Right (remaining, _, msg) -> (Just msg, toStrict remaining)
  where
    get = do size <- getInt32be
             frame <- getInt32be
             readFrame size frame

readFrame :: Int32 -> Int32 -> Get Packet
readFrame size frame
    | frame == 0 = ResponsePacket <$> getByteString (fromIntegral size - 4)
    | frame == 1 = ErrorPacket <$> getByteString (fromIntegral size - 4)
    | otherwise = MessagePacket
        <$> (Message <$> getInt64be <*> getInt16be <*> getByteString 16
             <*> getByteString (fromIntegral size - 30))

toStrict :: BL.ByteString -> B.ByteString
toStrict = B.concat . BL.toChunks
