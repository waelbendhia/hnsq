module Network.NSQ.LookupDResponse
    ( address
    , LookupDResponse(..)
    , prAddress
    , ProducerResponse(..)
    , queryLookupD
    , LookupException(..)
    ) where

import           Control.Exception
import           Control.Lens         ( (^.), view )

import           Data.Aeson
import           Data.Bifunctor
import qualified Data.ByteString.Lazy as BL
import           Data.Text            ( Text, pack, unpack )

import           GHC.Generics

import           Network.HTTP.Client
                 ( HttpException(..), HttpExceptionContent(..) )
import           Network.Wreq

newtype LookupDResponse = LookupDResponse [ProducerResponse]
    deriving ( Show )

data ProducerResponse =
    ProducerResponse { prBroadcastAddress :: Text
                     , prHostname         :: Text
                     , prRemoteAddress    :: Text
                     , prTcpPort          :: Int
                     , prHttpPort         :: Int
                     , prVersion          :: Text
                     }
    deriving ( Show, Generic )

instance FromJSON ProducerResponse where
    parseJSON = genericParseJSON defaultOptions { fieldLabelModifier =
                                                      camelTo2 '_' . drop 2
                                                }

instance FromJSON LookupDResponse where
    parseJSON = withObject "LookupDResponse" $ \o -> do
        LookupDResponse <$> o .: "producers"

prAddress :: ProducerResponse -> Text
prAddress ProducerResponse{..} = address prHostname $ pack (show prTcpPort)

-- | Address from host + port.
address :: Text -> Text -> Text
address host port = host <> ":" <> port

data LookupException = ParseException | TopicNotFoundException
    deriving Show

instance Exception LookupException

-- | From a given looupd url, return availble producers
queryLookupD :: Text -> Text -> IO (Either SomeException LookupDResponse)
queryLookupD url topic = do
    r <- try (get (unpack (url <> "/lookup?topic=" <> topic)))
    pure $ first handleHTTPException $ parseRequestBody r
  where
    parseRequestBody r = do res <- r
                            maybe (Left $ toException ParseException) Right $
                                decode (view responseBody res)
    handleHTTPException e = case fromException e of
        Just (HttpExceptionRequest _ (StatusCodeException res _)) -> if res
            ^. responseStatus . statusCode == 404
            then toException TopicNotFoundException
            else e
        Nothing -> e
