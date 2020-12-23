module Network.NSQ.Commands (fin, magic, mpub, nop, rdy, req, sub, pub) where

import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as C8
import qualified Data.ByteString.Lazy as BL
import           Data.ByteString.UTF8 as BSU
import           Data.Binary (encode)
import           Data.Int

fin :: B.ByteString -> B.ByteString
fin msgId = "FIN " <> msgId <> "\n"

magic :: B.ByteString
magic = "  V2"

mpub :: B.ByteString -> [B.ByteString] -> B.ByteString
mpub topic payload = "MPUB "
  <> topic
  <> "\n"
  <> toStrict (encode totalSize)
  <> B.concat (fmap message payload)
  where
    len bytes = (fromIntegral $ B.length bytes) :: Int32

    totalSize = sum $ fmap len payload

    message bytes = toStrict (encode (len bytes)) <> bytes

nop :: B.ByteString
nop = "NOP\n"

rdy :: Int -> B.ByteString
rdy count = "RDY " <> BSU.fromString (show count) <> "\n"

req :: B.ByteString -> B.ByteString
req msgId = "REQ " <> msgId <> " " <> C8.pack (show (1000 :: Int32))

sub :: B.ByteString -> B.ByteString -> B.ByteString
sub topic chan = "SUB " <> topic <> " " <> chan <> "\n"

pub :: B.ByteString -> B.ByteString -> B.ByteString
pub topic bytes = "PUB " <> topic <> "\n" <> toStrict (encode l) <> bytes
  where
    l = (fromIntegral $ B.length bytes) :: Int32

toStrict :: BL.ByteString -> B.ByteString
toStrict = B.concat . BL.toChunks
