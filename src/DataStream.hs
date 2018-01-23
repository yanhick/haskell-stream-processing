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
import Data.IORef
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
  Partition
    :: (Show a, Show b, Binary a, Binary b)
    => (a -> Int)
    -> DataStream a b
    -> DataStream a b
  Fold
    :: (Show a, Show b, Binary a, Binary b)
    => (a -> b -> a)
    -> a
    -> DataStream a b
    -> DataStream a b
  Identity :: Binary a => DataStream a a

data DataStreamInternal
  = MapInternal (LB.ByteString -> LB.ByteString)
  | FilterInternal (LB.ByteString -> Bool)
  | FlatMapInternal (LB.ByteString -> Maybe [LB.ByteString])

data KeyedDataStream a b where
  KeyBy :: Hashable c => (a -> c) -> DataStream a b -> KeyedDataStream a b

data DataStreamOperationInternal
  = DSInternal DataStreamInternal
  | PartitionInternal (LB.ByteString -> Int)
  | FoldInternal ((LB.ByteString, LB.ByteString) -> LB.ByteString) LB.ByteString

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

data Sink a where
  SinkFile :: Binary a => FilePath -> (a -> B.ByteString) -> Sink a
  StdOut :: Binary a => (a -> B.ByteString) -> Sink a

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

getPlan :: DataStream a b -> [DataStreamOperationInternal]
getPlan (Map f cont) = (DSInternal $ MapInternal (encode . f . decode)) : getPlan cont
getPlan (Filter f cont) = (DSInternal $ FilterInternal (f . decode)) : getPlan cont
getPlan (FlatMap f cont) =
  (DSInternal $ FlatMapInternal (pure . fmap encode . f . decode)) : getPlan cont
getPlan Identity = []
getPlan (Partition f cont) = PartitionInternal (f . decode) : getPlan cont
getPlan (Fold f initValue cont) = FoldInternal
  (\(acc, v) -> encode $ f (decode acc) (decode v)) (encode initValue) : getPlan cont

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


type OperatorsToRun = [Int]

type RemoteTaskManagers = [ProcessId]
data TaskManagerRunPlan =
  TaskManagerRunPlan OperatorsToRun
                     [(Int, [ProcessId])]

newtype TaskId =
  TaskId Int
  deriving (Generic, Show)

instance Binary TaskId

runTaskId :: TaskId -> Int
runTaskId (TaskId i) = i

type IndexedPlan = [(Int, DataStreamOperationInternal)]

getIndexedPlan :: DataStream a b -> IndexedPlan
getIndexedPlan ds = zip [1 ..] (getPlan ds)

getMergedIndexedPlan :: IndexedPlan -> [Int] -> IndexedPlan
getMergedIndexedPlan indexedPlan ids =
  filter (\(taskId, _) -> taskId `elem` ids) indexedPlan

getMergedProcessIdMap ::
     [(Int, [ProcessId])] -> [(Int, [ProcessId])] -> ProcessIdMap
getMergedProcessIdMap local remote =
  MapContainer.fromListWith (++) (local ++ remote)

runTaskManager :: (Binary a, Binary b, Show b) => TaskManagerRunPlan -> Pipeline a b -> Process ()
runTaskManager (TaskManagerRunPlan ids processIds) (Pipeline source ds sink) = do
  let plans = getMergedIndexedPlan (getIndexedPlan ds) ids
  selfPid <- getSelfPid
  communicationManagerPid <- spawnLocal $ runCommunicationManager selfPid
  sendPort <- expect :: Process CommunicationManagerPort
  nodes <-
    forM plans $ \(operatorId, ds') -> do
      processId <-
        spawnLocal $ case ds' of
          DSInternal ds'' -> runTransformTask (TaskId operatorId) sendPort ds''
          PartitionInternal f -> runPartitionTask (TaskId operatorId) sendPort f
          FoldInternal f initValue -> runFoldTask (TaskId operatorId) sendPort f initValue
      return (operatorId, [processId])
  sourceNode <-
    spawnLocal $ runSourceTask (TaskId 0) sendPort source
  sinkNode <- spawnLocal $ runSinkTask sink
  let sinkNodeIndex = length (getIndexedPlan ds) + 1
  let allProcessIds =
        getMergedProcessIdMap
          ((0, [sourceNode]) : (sinkNodeIndex, [sinkNode]) : nodes)
          processIds
  send communicationManagerPid allProcessIds
  forM_ nodes (\(_, [nodePid]) -> send nodePid allProcessIds)

runCommunicationManager :: ProcessId -> Process ()
runCommunicationManager pid = do
  (sendPort, receivePort) <- newChan :: Process (SendPort CommunicationManagerMessage, ReceivePort CommunicationManagerMessage)
  send pid sendPort
  allProcessIds <- expect :: Process ProcessIdMap
  forever $ do
    msg <- receiveChan receivePort
    case msg of
      TaskResult (TaskId operatorId) value -> do
        let nextOperator = MapContainer.lookup (operatorId + 1) allProcessIds
        case nextOperator of
          (Just (x:_)) -> send x (IntermediateResult value)
          _            -> return ()
      PartitionResult _ pid' value -> send pid' (IntermediateResult value)

newtype PartitionBucket = PartitionBucket Int
  deriving (Generic)

instance Binary PartitionBucket where

data CommunicationManagerMessage = TaskResult TaskId LB.ByteString | PartitionResult TaskId ProcessId LB.ByteString
  deriving (Generic, Show)

instance Binary CommunicationManagerMessage where

type CommunicationManagerPort = SendPort CommunicationManagerMessage

newtype IntermediateResult = IntermediateResult LB.ByteString
  deriving (Generic)

instance Binary IntermediateResult where

runFoldTask :: TaskId -> CommunicationManagerPort -> ((LB.ByteString, LB.ByteString) -> LB.ByteString) -> LB.ByteString -> Process ()
runFoldTask taskId sendPort f initValue = do
    prevResult <- liftIO $ newIORef (initValue :: LB.ByteString)
    forever $ do
      (IntermediateResult value) <- expect :: Process IntermediateResult
      prev <- liftIO $ readIORef prevResult
      let res = f (prev, value)
      liftIO $ writeIORef prevResult res
      sendChan sendPort $ TaskResult taskId res

runPartitionTask :: Binary a => TaskId -> CommunicationManagerPort -> (a ->  Int) -> Process ()
runPartitionTask taskId sendPort f = do
  processIdMap <- expect :: Process ProcessIdMap
  let processesForTask = MapContainer.lookup (runTaskId taskId) processIdMap
  forever $ do
    (IntermediateResult value) <- expect :: Process IntermediateResult
    let decoded = decode value
    case processesForTask of 
      Just pTasks -> do
        let processIndex = f decoded `mod` length pTasks
        let processId = pTasks !! processIndex
        sendChan sendPort (PartitionResult taskId processId value)
      Nothing -> return ()

runTransformTask :: TaskId -> CommunicationManagerPort -> DataStreamInternal -> Process ()
runTransformTask taskId sendPort ds =
  forever $ do
    (IntermediateResult value) <- expect :: Process IntermediateResult
    let res = runDataStreamInternal ds value
    case res of
      Just res' -> forM_ res' (sendChan sendPort . TaskResult taskId)
      _         -> return ()

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
                _                                            -> B.empty
        sendChan sendPort $ TaskResult taskId (encode $ dec $ LB.fromStrict value'')
