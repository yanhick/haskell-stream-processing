module Pipeline where
import Data.ByteString.Lazy
import Data.Text
import Operation

data KafkaConsumerConfig = KafkaConsumerConfig
  { topicName     :: String
  , brokerHost    :: String
  , brokerPort    :: Int
  , consumerGroup :: String
  }

data Source a
  = Collection [a]
  | SourceFile FilePath
               (Text -> a)
  | StdIn (Text -> a)
  | SourceKafkaTopic KafkaConsumerConfig
                     (ByteString -> a)

data Sink a 
  = SinkFile FilePath (a -> ByteString)
  | StdOut (a -> ByteString)

data Pipeline a b =
  Pipeline (Source a)
           (Operation a b)
           (Sink b)
