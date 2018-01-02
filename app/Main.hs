{-# LANGUAGE TemplateHaskell, StandaloneDeriving, RankNTypes, DeriveGeneric, GADTs, StaticPointers #-}
module Main where

import GHC.Generics
import Data.Binary
import Data.Typeable
import DataStream
import System.Environment (getArgs)
import Control.Distributed.Process
import Control.Distributed.Process.Closure
import Control.Distributed.Process.Node (initRemoteTable)
import Control.Distributed.Process.Backend.SimpleLocalnet

data StreamData = Hello Int | Goodbye String
  deriving (Typeable, Generic, Show)

instance Binary StreamData where

dataStream :: DataStream StreamData Int
dataStream = Map doubleCl $ Map doubleInt $ Map doubleInt Identity

pipeline :: Pipeline StreamData Int
pipeline = Pipeline (Collection [Hello 5, Goodbye "hello"]) dataStream (File "/tmp/hello")

runStream' :: [StreamData] -> Process ()
runStream' ds =
 runPipeline ds pipeline


doubleInt :: Int -> Int
doubleInt i = i * 2

doubleCl :: StreamData -> Int
doubleCl (Hello a) = a * 2
doubleCl (Goodbye _) = 0

remotable ['runStream']

myRemoteTable :: RemoteTable
myRemoteTable =
  Main.__remoteTable
  initRemoteTable


main :: IO ()
main = do
  args <- getArgs

  case args of
    ["master", host, port] -> do
      backend <- initializeBackend host port myRemoteTable
      startMaster backend (DataStream.startPipeline pipeline $(mkClosure 'runStream'))
    ["slave", host, port] -> do
      backend <- initializeBackend host port myRemoteTable
      startSlave backend
    _ -> return ()
