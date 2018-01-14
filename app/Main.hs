{-# LANGUAGE DeriveGeneric      #-}
{-# LANGUAGE GADTs              #-}
{-# LANGUAGE RankNTypes         #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE StaticPointers     #-}
{-# LANGUAGE TemplateHaskell    #-}

module Main where

import           Control.Distributed.Process
import           Control.Distributed.Process.Backend.SimpleLocalnet
import           Control.Distributed.Process.Closure
import           Control.Distributed.Process.Node                   (initRemoteTable)
import           Data.Binary
import qualified Data.ByteString                                    as B
import qualified Data.ByteString.Lazy                               as BL
import           Data.Typeable
import           DataStream
import           GHC.Generics
import           System.Environment                                 (getArgs)

data StreamData
  = Hello Int
  | Goodbye String
  deriving (Typeable, Generic, Show)

instance Binary StreamData

data OutputStreamData
  = Hello' Int
  | Goodbye' String
  deriving (Typeable, Generic, Show)

instance Binary OutputStreamData

iToO :: StreamData -> OutputStreamData
iToO (Hello i)   = Hello' i
iToO (Goodbye s) = Goodbye' s

dataStream :: DataStream StreamData OutputStreamData
dataStream = Map iToO Identity

kafkaConsumerConfig :: KafkaConsumerConfig
kafkaConsumerConfig =
  KafkaConsumerConfig
  { topicName = "test"
  , brokerHost = "localhost"
  , brokerPort = 9092
  , consumerGroup = "test_group"
  }

source :: Source StreamData
source = SourceKafkaTopic kafkaConsumerConfig (Goodbye . show)

sink :: Sink OutputStreamData
sink = StdOut $ B.concat . BL.toChunks . encode

pipeline :: Pipeline StreamData OutputStreamData
pipeline = Pipeline source dataStream sink

startTaskManager :: [NodeId] -> Process ()
startTaskManager peers = runTaskManager peers pipeline

doubleInt :: Int -> Int
doubleInt i = i * 2

doubleCl :: StreamData -> Int
doubleCl (Hello a)   = a * 2
doubleCl (Goodbye _) = 0

numToZero :: Int -> [String]
numToZero n = fmap show [0 .. n]

remotable ['startTaskManager]

myRemoteTable :: RemoteTable
myRemoteTable = Main.__remoteTable initRemoteTable

main :: IO ()
main = do
  args <- getArgs
  case args of
    ["tm", host, port] -> do
      backend <- initializeBackend host port myRemoteTable
      startSlave backend
    ["jm", host, port] -> do
      backend <- initializeBackend host port myRemoteTable
      runJobManager backend pipeline $(mkClosure 'startTaskManager)
    _ -> return ()
