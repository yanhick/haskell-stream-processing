{-# LANGUAGE RankNTypes, GADTs #-}
module DataStream where

import Data.Binary
import qualified Conduit as C
import qualified Data.Conduit.Text as TC
import qualified Data.Conduit.List as LC
import Data.Maybe
import qualified Data.Text as T
import Data.Typeable
import Control.Distributed.Process.Backend.SimpleLocalnet
import Control.Distributed.Process
import Control.Distributed.Process.Node (runProcess)
import Control.Monad (forM_)

data DataStream a b where
  Map :: (Show a, Show b, Show c) => (a -> b) -> DataStream b c -> DataStream a c
  Filter :: (Show a, Show b) => (a -> Bool) -> DataStream a b -> DataStream a b
  FlatMap :: (Show a, Show b, Show c) => (a -> [b]) -> DataStream b c -> DataStream a c
  Identity :: DataStream a a

runDataStream :: DataStream a b -> a -> Maybe [b]
runDataStream (Map f cont) x =
  runDataStream cont (f x)
runDataStream (Filter f cont) x =
  if f x
  then runDataStream cont x
  else Nothing
runDataStream (FlatMap f cont) x =
  concat <$> traverse (runDataStream cont) (f x)
runDataStream Identity x = Just [x]

runPipeline :: (Show b) => [a] -> Pipeline a b -> Process ()
runPipeline ds (Pipeline _ dataStream sink)= do
  let res = concat $ catMaybes $ fmap (runDataStream dataStream) ds
  runSink sink res

runSink :: (Show a) => Sink a -> [a] -> Process ()
runSink (Log serialize) xs = say $ show $ fmap serialize xs
runSink (StdOut serialize) xs = liftIO $ print $ fmap serialize xs
runSink (SinkFile fp serialize) xs = do
  say $ "writing " ++ show xs ++ " to " ++ fp
  liftIO $ mapM_ (appendFile fp . flip (++) "\n" . serialize) xs


runSource :: C.MonadResource m => Source a -> C.ConduitM () a m ()
runSource (Collection xs) = LC.sourceList xs
runSource (SourceFile path deserialize) =
  C.sourceFile path C..| C.decodeUtf8C C..| TC.lines C..| C.mapC deserialize
runSource (StdIn deserialize) =
  C.stdinC C..| C.decodeUtf8C C..| TC.lines C..| C.mapC deserialize


data Source a = Collection [a] | SourceFile FilePath (T.Text -> a) | StdIn (T.Text -> a)

data Sink a = Log (a -> String) | SinkFile FilePath (a -> String) | StdOut (a -> String)

data Pipeline a b = Pipeline (Source a) (DataStream a b)  (Sink b)


startPipeline :: (Binary a, Typeable a, Show a) => String -> String -> RemoteTable -> Pipeline a b -> ([a] -> Closure (Process ())) -> IO ()
startPipeline host port remoteTable (Pipeline source _ _) start = do
  backend <- initializeBackend host port remoteTable
  node <- newLocalNode backend
  peers <- findPeers backend 1000000
  runProcess node $ forM_ peers $ \peer -> spawn peer (start [])
  _ <- C.runConduitRes
    $ runSource source C..| C.mapMC (\a -> do
      C.lift $ print a
      C.lift $ runProcess node $ forM_ peers $ \slave -> spawn slave (start [a])
    ) C..| C.sinkList
  return ()
