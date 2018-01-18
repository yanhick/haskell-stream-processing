{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE GADTs         #-}
{-# LANGUAGE LambdaCase    #-}
{-# LANGUAGE RankNTypes    #-}

module DataStream where

import           Control.Distributed.Process
import           Control.Distributed.Process.Backend.SimpleLocalnet
import           Control.Distributed.Process.Node                   (localNodeId,
                                                                     runProcess)
import           Control.Monad                                      (forM,
                                                                     forM_,
                                                                     forever)
import           Data.Binary
import qualified Data.ByteString                                    as B
import qualified Data.ByteString.Lazy                               as LB
import           Data.Hashable
import           Data.List
import qualified Data.Map                                           as MapContainer
import           Data.Monoid                                        ((<>))
import qualified Data.Text                                          as T
import           Data.Typeable
import           GHC.Generics
import           Kafka.Conduit.Source

data DataStream a b where
  Map
    :: (Show a, Show b, Show c, Binary a, Binary b)
    => (a -> b)
    -> DataStream b c
    -> DataStream a c
  Filter
    :: (Show a, Show b, Binary a, Binary b)
    => (a -> Bool)
    -> DataStream a b
    -> DataStream a b
  FlatMap
    :: (Show a, Show b, Show c, Binary a, Binary b, Binary c)
    => (a -> [b])
    -> DataStream b c
    -> DataStream a c
  Identity :: Binary a => DataStream a a

data DataStreamInternal
  = MapInternal (LB.ByteString -> LB.ByteString)
  | FilterInternal (LB.ByteString -> Bool)
  | FlatMapInternal (LB.ByteString -> Maybe [LB.ByteString])

data KeyedDataStream a b where
  KeyBy :: Hashable c => (a -> c) -> DataStream a b -> KeyedDataStream a b

data DataStreamOperation a b
  = Keyed (KeyedDataStream a b)
  | NonKeyed (DataStream a b)

data PipelineInternal a b =
  PipelineInternal (Source a)
                   DataStreamInternal
                   (Sink b)

data Source a
  = Collection [a]
  | SourceFile FilePath
               (T.Text -> a)
  | StdIn (T.Text -> a)
  | SourceKafkaTopic KafkaConsumerConfig
                     (LB.ByteString -> a)

data Sink a
  = SinkFile FilePath
             (a -> B.ByteString)
  | StdOut (a -> B.ByteString)

data Pipeline a b =
  Pipeline (Source a)
           (DataStream a b)
           (Sink b)

data KafkaConsumerConfig = KafkaConsumerConfig
  { topicName     :: String
  , brokerHost    :: String
  , brokerPort    :: Int
  , consumerGroup :: String
  }

runKeyedDataStream :: KeyedDataStream a b -> a -> Maybe [b]
runKeyedDataStream _ _ = Nothing

runDataStreamInternal ::
     DataStreamInternal -> LB.ByteString -> Maybe [LB.ByteString]
runDataStreamInternal (MapInternal f) x = Just [f x]
runDataStreamInternal (FilterInternal f) x =
  if f x
    then Just [x]
    else Nothing
runDataStreamInternal (FlatMapInternal f) x = f x

runDataStreamEncoded ::
     DataStreamInternal -> LB.ByteString -> Maybe [LB.ByteString]
runDataStreamEncoded = runDataStreamInternal

getPlan :: DataStream a b -> [DataStreamInternal]
getPlan (Map f cont) = MapInternal (encode . f . decode) : getPlan cont
getPlan (Filter f cont) = FilterInternal (f . decode) : getPlan cont
getPlan (FlatMap f cont) =
  FlatMapInternal (pure . fmap encode . f . decode) : getPlan cont
getPlan Identity = []

runJobManager ::
     (Binary a, Typeable a, Show a)
  => Backend
  -> Pipeline a b
  -> ([NodeId] -> Closure (Process ()))
  -> IO ()
runJobManager backend _ start = do
  node <- newLocalNode backend
  nodes <- findPeers backend 1000000
  let peers = delete (localNodeId node) nodes
  runProcess node $ forM_ peers $ \peer -> spawn peer (start peers)

kafkaBroker :: String -> Int -> BrokerAddress
kafkaBroker host port = BrokerAddress $ host ++ ":" ++ show port

consumerProps :: BrokerAddress -> String -> ConsumerProperties
consumerProps kafkaBroker' consumerGroup' =
  brokersList [kafkaBroker'] <> groupId (ConsumerGroupId consumerGroup') <>
  noAutoCommit

consumerSub :: String -> Subscription
consumerSub topicName' = topics [TopicName topicName'] <> offsetReset Earliest

data TaskManagerInfo =
  TaskManagerInfo ProcessId
                  Int
  deriving (Generic, Show)

instance Binary TaskManagerInfo

startTaskManager' :: Process ()
startTaskManager' = do
  (Just jmid) <- whereis "job-manager"
  pid <- getSelfPid
  send jmid (TaskManagerInfo pid 10)

type ProcessIdMap = MapContainer.Map Int [ProcessId]

data TaskManagerRunPlan =
  TaskManagerRunPlan [Int]
                     [(Int, [ProcessId])]

newtype TaskId =
  TaskId Int

runTaskId :: TaskId -> Int
runTaskId (TaskId i) = i

type IndexedPlan = [(Int, DataStreamInternal)]

getIndexedPlan :: DataStream a b -> IndexedPlan
getIndexedPlan ds = zip [1 ..] (getPlan ds)

getMergedIndexedPlan :: IndexedPlan -> [Int] -> IndexedPlan
getMergedIndexedPlan indexedPlan ids =
  filter (\(operatorId, _) -> operatorId `elem` ids) indexedPlan

getMergedProcessIdMap ::
     [(Int, [ProcessId])] -> [(Int, [ProcessId])] -> ProcessIdMap
getMergedProcessIdMap local remote =
  MapContainer.fromListWith (++) (local ++ remote)

runTaskManager :: Binary a => TaskManagerRunPlan -> Pipeline a b -> Process ()
runTaskManager (TaskManagerRunPlan ids processIds) (Pipeline source ds sink) = do
  let plans = getMergedIndexedPlan (getIndexedPlan ds) ids
  communicationManagerPid <- spawnLocal runCommunicationManager
  nodes <-
    forM plans $ \(operatorId, ds') -> do
      processId <-
        spawnLocal $
        runTransformTask (TaskId operatorId) communicationManagerPid ds'
      return (operatorId, [processId])
  sourceNode <-
    spawnLocal $ runSourceTask (TaskId 0) communicationManagerPid source
  sinkNode <- spawnLocal $ runSinkTask sink
  let sinkNodeIndex = length (getIndexedPlan ds) + 1
  let allProcessIds =
        getMergedProcessIdMap
          ((0, [sourceNode]) : (sinkNodeIndex, [sinkNode]) : nodes)
          processIds
  send communicationManagerPid allProcessIds

runCommunicationManager :: Process ()
runCommunicationManager = do
  allProcessIds <- expect :: Process ProcessIdMap
  forever $ do
    (operatorId, value) <- expect :: Process (Int, LB.ByteString)
    let nextOperator = MapContainer.lookup (operatorId + 1) allProcessIds
    case nextOperator of
      (Just (x:_)) -> send x value
      _            -> return ()

runTransformTask :: TaskId -> ProcessId -> DataStreamInternal -> Process ()
runTransformTask taskId pid ds =
  forever $ do
    value <- expect :: Process LB.ByteString
    let res = runDataStreamInternal ds value
    case res of
      Just res' -> forM_ res' (\res'' -> send pid (runTaskId taskId, res''))
      _         -> return ()

runSinkTask :: Sink a -> Process ()
runSinkTask (SinkFile path _) =
  forever $ do
    value <- expect :: Process LB.ByteString
    liftIO $ appendFile path (show value)
runSinkTask (StdOut _) =
  forever $ do
    value <- expect :: Process LB.ByteString
    liftIO $ print value

runSourceTask :: (Binary a) => TaskId -> ProcessId -> Source a -> Process ()
runSourceTask taskId pid (Collection xs) =
  forM_ xs (\x -> send pid (runTaskId taskId, encode x))
runSourceTask taskId pid (SourceFile path _) = do
  lines' <- liftIO $ fmap lines (readFile path)
  forM_ lines' (\line -> send pid (runTaskId taskId, line))
runSourceTask taskId pid (StdIn _) =
  forever $ do
    line <- liftIO getLine
    send pid (runTaskId taskId, line)
runSourceTask taskId pid (SourceKafkaTopic KafkaConsumerConfig { topicName = topicName'
                                                               , brokerHost = brokerHost'
                                                               , brokerPort = brokerPort'
                                                               , consumerGroup = consumerGroup'
                                                               } _) = do
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
                _                                            -> B.empty
        send pid (runTaskId taskId, encode value'')
