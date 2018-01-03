{-# LANGUAGE RankNTypes, GADTs #-}
module DataStream where

import Data.Binary
import System.IO
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

runSink :: (Show a) => Sink a -> [a] -> Process ()
runSink (Log serialize) xs = say $ show $ fmap serialize xs
runSink (StdOut serialize) xs = liftIO $ print $ fmap serialize xs
runSink (SinkFile fp serialize) xs = do
  say $ "writing " ++ show xs ++ " to " ++ fp
  liftIO $ mapM_ (appendFile fp . flip (++) "\n" . serialize) xs


runSource :: Source a -> IO [a]
runSource (Collection xs) = return xs
runSource (SourceFile path deserialize) = do
  contents <- readFile path
  return $ deserialize <$> lines contents
runSource (StdIn deserialize) = do
  end <- isEOF
  if end
    then return []
  else do
    line <- getLine
    print line
    do 
      lines' <- runSource (StdIn deserialize)
      return (deserialize line : lines')


data Source a = Collection [a] | SourceFile FilePath (String -> a) | StdIn (String -> a)

data Sink a = Log (a -> String) | SinkFile FilePath (a -> String) | StdOut (a -> String)

data Pipeline a b = Pipeline (Source a) (DataStream a b)  (Sink b)

startPipeline :: (Binary a, Typeable a, Show a) => String -> String -> RemoteTable -> Pipeline a b -> ([a] -> Closure (Process ())) -> IO ()
startPipeline host port remoteTable (Pipeline source _ _ ) start = do
  backend <- initializeBackend host port remoteTable
  source' <- runSource source
  startMaster backend (spawnSlaves source' start)

spawnSlaves :: (Binary a, Typeable a, Show a) => [a] -> ([a] -> Closure (Process ())) -> [NodeId] -> Process ()
spawnSlaves source start slaves = do
  forM_ (zip source (cycle slaves))$ \(d, slave) -> spawn slave (start [d])
  _ <- expect :: Process ()
  return ()
