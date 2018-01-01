{-# LANGUAGE TemplateHaskell, DeriveGeneric, GADTs, StaticPointers #-}
module Main where

import GHC.Generics
import System.IO.Unsafe
import GHC.StaticPtr
import Data.Binary
import Data.Typeable
import Data.List
import System.Environment (getArgs)
import Control.Distributed.Process
import Control.Distributed.Process
import Control.Distributed.Static (staticLabel, registerStatic, closureApply, staticPtr, staticClosure)
import Control.Distributed.Process.Closure
import Control.Distributed.Process.Node (initRemoteTable, runProcess)
import Control.Distributed.Process.Backend.SimpleLocalnet
import Control.Monad (forever, forM_)

data DataStream a b = Map (a -> b)

data StreamData = Hello Int | Goodbye String
  deriving (Typeable, Generic)

instance Binary StreamData where

dataStream :: DataStream StreamData Int
dataStream = Map doubleCl

runDataStream :: Show b => DataStream a b -> a -> Process ()
runDataStream (Map f) i = do
  res <- return $ f i
  liftIO $ print res

runStream :: StreamData -> Process ()
runStream int =
  runDataStream dataStream int


doubleCl :: StreamData -> Int
doubleCl (Hello a) = a * 2
doubleCl (Goodbye s) = 0

remotable ['runStream]

myRemoteTable :: RemoteTable
myRemoteTable =
  Main.__remoteTable
  $ initRemoteTable

dataStreamMaster :: Backend -> [NodeId] -> Process ()
dataStreamMaster backend slaves = do
  liftIO . putStrLn $ "slaves: " ++ show slaves
  slaveProcesses <- findSlaves backend
  pid <- getSelfPid
  forM_ slaves $ \slave -> do
    child <- spawn slave ($(mkClosure 'runStream)(Hello 5))
    res <- expect
    liftIO (print (res :: [Int]))


main :: IO ()
main = do
  args <- getArgs

  case args of
    ["master", host, port] -> do
      backend <- initializeBackend host port myRemoteTable
      startMaster backend (dataStreamMaster backend)
    ["slave", host, port] -> do
      backend <- initializeBackend host port myRemoteTable
      startSlave backend
