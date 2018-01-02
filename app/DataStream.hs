{-# LANGUAGE RankNTypes, GADTs #-}
module DataStream where

import Data.Binary
import Data.Maybe
import Data.Typeable
import Control.Distributed.Process
import Control.Monad (forM_)

data DataStream a b where
  Map :: (Show a, Show b, Show c) => (a -> b) -> DataStream b c -> DataStream a c
  Filter :: (Show a, Show b) => (a -> Bool) -> DataStream a b -> DataStream a b
  Identity :: DataStream a a

runDataStream :: DataStream a b -> a -> Maybe b
runDataStream (Map f cont) x =
  runDataStream cont (f x)
runDataStream (Filter f cont) x =
  if f x
  then runDataStream cont x
  else Nothing
runDataStream Identity x = Just x

runPipeline :: (Show b) => [a] -> Pipeline a b -> Process ()
runPipeline ds (Pipeline _ dataStream sink)= do
  let res = catMaybes $ fmap (runDataStream dataStream) ds
  runSink sink res

runSink :: (Show a) => Sink -> [a] -> Process()
runSink Log xs = say $ show xs
runSink (File fp) xs = do
  say $ "writing " ++ show xs ++ " to " ++ fp
  liftIO $ mapM_ (appendFile fp . flip (++) "\n" . show) xs

newtype Source a = Collection [a]

data Sink = Log | File FilePath

data Pipeline a b = Pipeline (Source a) (DataStream a b)  Sink

startPipeline :: (Binary a, Typeable a) => Pipeline a b -> ([a] -> Closure (Process ())) -> [NodeId] -> Process ()
startPipeline (Pipeline (Collection col) _ _) cl slaves = do
  forM_ slaves $ \slave -> spawn slave (cl col)
  _ <- expect :: Process Int
  return ()
