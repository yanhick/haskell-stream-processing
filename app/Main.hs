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
import           Data.Typeable
import           JobManager
import           TaskManager
import           Operation
import           Pipeline
import           GHC.Generics
import           System.Environment                                 (getArgs)

data StreamData
  = Hello Int
  | Goodbye String
  deriving (Typeable, Generic, Show)

instance Binary StreamData  where
  


data OutputStreamData
  = Hello' Int
  | Goodbye' String
  deriving (Typeable, Generic, Show)

instance Binary OutputStreamData

iToO :: StreamData -> OutputStreamData
iToO (Hello i)   = Hello' i
iToO (Goodbye s) = Goodbye' s

dataStream :: Operation StreamData Int
dataStream = Map iToO $ Map (const 1) $ Fold (+) 0 Identity

kafkaConsumerConfig :: KafkaConsumerConfig
kafkaConsumerConfig =
  KafkaConsumerConfig
  { topicName = "test"
  , brokerHost = "localhost"
  , brokerPort = 9092
  , consumerGroup = "test_group"
  }

source :: Source StreamData
source = SourceKafkaTopic kafkaConsumerConfig (const $ Hello 2)

sink :: Sink Int
sink = StdOut encode

pipeline :: Pipeline StreamData Int
pipeline = Pipeline source dataStream sink

startTaskManager :: [NodeId] -> Process ()
startTaskManager _ = runTaskManager (TaskManagerRunPlan [1, 2, 3] []) pipeline

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
