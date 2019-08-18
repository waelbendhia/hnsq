# hnsq - Haskell NSQ client

Example:
```haskell
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}

import Network.NSQ
import Data.Text.Encoding as TSE
import Control.Monad
import Control.Concurrent

main :: IO ()
main = do
  conn <- connectNSQ "topic" "channel" "127.0.0.1" "4150"
  -- or
  conn <- connectNSQLookupD "http://localhost:4161" "topic" "channel"

  -- Add handler
  withConnection conn $ \Message{..} -> do
    print (TSE.decodeUtf8 msgBytes)
    return Nothing

  -- Publish message
  publish conn "Some message"
  forever $ threadDelay 1000000

```
