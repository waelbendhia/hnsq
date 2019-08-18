module Network.NSQ.LookupDResponse
    (address
    , LookupDResponse(..)
    , prAddress
    , ProducerResponse(..)
    , queryLookupD
    ) where

import Data.Text (Text, pack, unpack)
import Data.Aeson
import GHC.Generics

import Control.Lens ((^.))
import Network.Wreq
import Control.Exception

import qualified Data.ByteString.Lazy as BL

data LookupDResponse =
  LookupDResponse [ProducerResponse]
  deriving (Show)

data ProducerResponse = ProducerResponse
  { prBroadcastAddress :: Text
  , prHostname :: Text
  , prRemoteAddress :: Text
  , prTcpPort :: Int
  , prHttpPort :: Int
  , prVersion :: Text
  } deriving (Show, Generic)

instance FromJSON ProducerResponse where
  parseJSON = genericParseJSON defaultOptions {
                fieldLabelModifier = camelTo2 '_' . drop 2}

instance FromJSON LookupDResponse where
  parseJSON = withObject "LookupDResponse" $ \o -> do
    LookupDResponse <$> o .: "producers"

prAddress :: ProducerResponse -> Text
prAddress ProducerResponse{..} =
  address prHostname $ pack (show prTcpPort)

-- | Address from host + port.
address :: Text -> Text -> Text
address host port = host <> ":" <> port

-- | From a given looupd url, return availble producers
queryLookupD :: Text -> Text -> IO (Either String LookupDResponse)
queryLookupD url topic = do
  r <- try (get (unpack (url <> "/lookup?topic=" <> topic))) :: IO (Either SomeException (Response BL.ByteString))
  case r of
    Left err -> return $ Left $ show err
    Right res -> case decode (res ^. responseBody) of
                   Nothing -> return $ Left "Unexpected body."
                   Just ld -> return $ Right ld
  
