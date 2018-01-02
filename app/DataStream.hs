{-# LANGUAGE RankNTypes, GADTs #-}
module DataStream where

import Data.Binary
import Data.Maybe
import Data.Typeable
import Control.Distributed.Process.Backend.SimpleLocalnet
import Control.Distributed.Process
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

runSink :: (Show a) => Sink a -> [a] -> Process()
runSink Log xs = say $ show xs
runSink (SinkFile fp serialize) xs = do
  say $ "writing " ++ show xs ++ " to " ++ fp
  liftIO $ mapM_ (appendFile fp . flip (++) "\n" . serialize) xs


runSource :: Source a -> IO [a]
runSource (Collection xs) = return xs
runSource (SourceFile path deserialize) = do
  contents <- readFile path
  return $ deserialize <$> lines contents

data Source a = Collection [a] | SourceFile FilePath (String -> a)

data Sink a = Log | SinkFile FilePath (a -> String)

data Pipeline a b = Pipeline (Source a) (DataStream a b)  (Sink b)

startPipeline :: (Binary a, Typeable a, Show a) => String -> String -> RemoteTable -> Pipeline a b -> ([a] -> Closure (Process ())) -> IO ()
startPipeline host port remoteTable (Pipeline source _ _ ) start = do
  backend <- initializeBackend host port remoteTable
  source' <- runSource source
  startMaster backend (spawnSlaves source' start)

spawnSlaves :: (Binary a, Typeable a, Show a) => [a] -> ([a] -> Closure (Process ())) -> [NodeId] -> Process ()
spawnSlaves source start slaves= do
  forM_ slaves $ \slave -> spawn slave (start source)
  _ <- expect :: Process Int
  return ()
