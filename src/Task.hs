module Task where

import Pipeline
import Data.Monoid
import Data.IORef
import Data.Binary
import Control.Distributed.Process
import qualified Data.ByteString.Lazy as B
import qualified Data.ByteString as BS
import Data.Map as M
import Control.Monad
import CommunicationManager
import Operation
import Kafka.Consumer

runParDoTask :: TaskId -> CommunicationManagerPort -> SerializingParDo -> Process ()
runParDoTask taskId sendPort op =
  forever $ do
    (IntermediateResult value) <- expect :: Process IntermediateResult
    let res = runSerializingParDo op value
    case res of
      Just res' -> forM_ res' (sendChan sendPort . TaskResult taskId)
      _         -> return ()

runFoldTask :: TaskId -> CommunicationManagerPort -> ((B.ByteString, B.ByteString) -> B.ByteString) -> B.ByteString -> Process ()
runFoldTask taskId sendPort f initValue = do
    prevResult <- liftIO $ newIORef (initValue :: B.ByteString)
    forever $ do
      (IntermediateResult value) <- expect :: Process IntermediateResult
      prev <- liftIO $ readIORef prevResult
      let res = f (prev, value)
      liftIO $ writeIORef prevResult res
      sendChan sendPort $ TaskResult taskId res

runPartitionTask :: Binary a => TaskId -> CommunicationManagerPort -> (a ->  Int) -> Process ()
runPartitionTask taskId sendPort f = do
  processIdMap <- expect :: Process ProcessIdMap
  let processesForTask = M.lookup (runTaskId taskId) processIdMap
  forever $ do
    (IntermediateResult value) <- expect :: Process IntermediateResult
    let decoded = decode value
    case processesForTask of 
      Just pTasks -> do
        let processIndex = f decoded `mod` length pTasks
        let processId = pTasks !! processIndex
        sendChan sendPort (PartitionResult taskId processId value)
      Nothing -> return ()

runSinkTask :: (Binary a, Show a) => Sink a -> Process ()
runSinkTask (SinkFile path _) =
  forever $ do
    (IntermediateResult value) <- expect :: Process IntermediateResult
    liftIO $ appendFile path (show value)
runSinkTask (StdOut enc) =
  forever $ do
    (IntermediateResult value) <- expect :: Process IntermediateResult
    let decoded = decode value
    _ <- return $ enc decoded
    liftIO $ print decoded

kafkaBroker :: String -> Int -> BrokerAddress
kafkaBroker host port = BrokerAddress $ host ++ ":" ++ show port

consumerProps :: BrokerAddress -> String -> ConsumerProperties
consumerProps kafkaBroker' consumerGroup' =
  brokersList [kafkaBroker'] <> groupId (ConsumerGroupId consumerGroup') <>
  noAutoCommit

consumerSub :: String -> Subscription
consumerSub topicName' = topics [TopicName topicName'] <> offsetReset Earliest

runSourceTask :: (Binary a) => TaskId -> CommunicationManagerPort -> Source a -> Process ()
runSourceTask taskId sendPort (Collection xs) =
  forM_ xs (sendChan sendPort . TaskResult taskId . encode)
runSourceTask taskId sendPort (SourceFile path _) = do
  lines' <- liftIO $ fmap lines (readFile path)
  forM_ lines' (sendChan sendPort . TaskResult taskId . encode)
runSourceTask taskId sendPort (StdIn _) =
  forever $ do
    line <- liftIO getLine
    sendChan sendPort $ TaskResult taskId (encode line)
runSourceTask taskId sendPort (SourceKafkaTopic KafkaConsumerConfig { topicName = topicName'
                                                               , brokerHost = brokerHost'
                                                               , brokerPort = brokerPort'
                                                               , consumerGroup = consumerGroup'
                                                               } dec) = do
  consumer <-
    liftIO $
    newConsumer
      (consumerProps (kafkaBroker brokerHost' brokerPort') consumerGroup')
      (consumerSub topicName')
  case consumer of
    Left _ -> return ()
    Right consumer' ->
      forever $ do
        value <- pollMessage consumer' (Timeout 1000)
        let value'' =
              case value of
                Right ConsumerRecord {crValue = Just value'} -> value'
                _                                            -> BS.empty
        sendChan sendPort $ TaskResult taskId (encode $ dec $ B.fromStrict value'')
