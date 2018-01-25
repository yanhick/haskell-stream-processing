module JobManager where
import Control.Distributed.Process
import           Control.Distributed.Process.Backend.SimpleLocalnet
import Pipeline
import Data.Binary
import           Data.Typeable
import           Control.Distributed.Process.Node                   (localNodeId,
                                                                     runProcess)
import Control.Monad
import           Data.List

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