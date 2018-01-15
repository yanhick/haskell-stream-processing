{-# LANGUAGE GADTs      #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE RankNTypes #-}

module DataStream where

import qualified Conduit                                            as C
import           Control.Distributed.Process
import           Control.Distributed.Process.Backend.SimpleLocalnet
import           Control.Distributed.Process.Node                   (localNodeId,
                                                                     runProcess)
import           Control.Monad                                      (forM,
                                                                     forM_)
import           Data.Binary
import qualified Data.ByteString                                    as B
import qualified Data.ByteString.Lazy                               as LB
import qualified Data.Conduit.List                                  as LC
import qualified Data.Conduit.Text                                  as TC
import           Data.Hashable
import           Data.List
import           Data.Monoid                                        ((<>))
import qualified Data.Text                                          as T
import           Data.Typeable
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

runTask ::
     (Binary a, Binary b) => [NodeId] -> PipelineInternal a b -> Process ()
runTask _ (PipelineInternal source dataStream sink) = do
  liftIO $ print "start task"
  _ <-
    liftIO $
    C.runConduitRes $
    runSource source C..| C.mapC encode C..|
    C.concatMapC (runDataStreamInternal dataStream) C..|
    C.concatMapC id C..|
    C.mapC decode C..|
    runSink sink
  return ()

type Payload = (Int, LB.ByteString)

receiveTaskMessage :: [(Int, ProcessId)] -> Message -> Process ()
receiveTaskMessage nodes msg = do
  Just (taskId, payload) <- unwrapMessage msg :: Process (Maybe Payload)
  case filter ((==) taskId . fst) nodes of
    [(_, pid)] -> send pid payload
    _          -> return ()

runTaskManager ::
     (Binary a, Typeable a, Show b, Binary b)
  => [NodeId]
  -> Pipeline a b
  -> Process ()
runTaskManager peers (Pipeline source dataStream sink) = do
  liftIO $ print "start task manager"
  let plans = zip [0 ..] (getPlan dataStream)
  nodes <-
    forM plans $ \(operatorId, plan) -> do
      processId <-
        spawnLocal $ runTask peers (PipelineInternal source plan sink)
      return (operatorId, processId)
  mypid <- getSelfPid
  send mypid (0 :: Int, encode "hello yall")
  receiveWait [matchAny $ receiveTaskMessage nodes]

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

runSink :: C.MonadResource m => Sink a -> C.ConduitM a c m ()
runSink (SinkFile path serialize) = C.mapC serialize C..| C.sinkFile path
runSink (StdOut serialize)        = C.mapC serialize C..| C.printC

runSource :: C.MonadResource m => Source a -> C.ConduitM () a m ()
runSource (Collection xs) = LC.sourceList xs
runSource (SourceFile path deserialize) =
  C.sourceFile path C..| C.decodeUtf8C C..| TC.lines C..| C.mapC deserialize
runSource (StdIn deserialize) =
  C.stdinC C..| C.decodeUtf8C C..| TC.lines C..| C.mapC deserialize
runSource (SourceKafkaTopic KafkaConsumerConfig { topicName = topicName'
                                                , brokerHost = brokerHost'
                                                , brokerPort = brokerPort'
                                                , consumerGroup = consumerGroup'
                                                } deserialize) =
  kafkaSource
    (consumerProps (kafkaBroker brokerHost' brokerPort') consumerGroup')
    (consumerSub topicName')
    (Timeout 1000) C..|
  C.mapC
    (\case
       Right ConsumerRecord {crValue = Just value} -> value
       _ -> B.empty) C..|
  C.mapC LB.fromStrict C..|
  C.mapC deserialize
