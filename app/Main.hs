{-# LANGUAGE TemplateHaskell, StandaloneDeriving, RankNTypes, DeriveGeneric, GADTs, StaticPointers #-}
module Main where

import GHC.Generics
import Data.Binary
import Data.Typeable
import DataStream
import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as BL
import System.Environment (getArgs)
import Control.Distributed.Process
import Control.Distributed.Process.Closure
import Control.Distributed.Process.Node (initRemoteTable)
import Control.Distributed.Process.Backend.SimpleLocalnet

data StreamData = Hello Int | Goodbye String
  deriving (Typeable, Generic, Show)
instance Binary StreamData where


data OutputStreamData = Hello' Int | Goodbye' String
  deriving (Typeable, Generic, Show)
instance Binary OutputStreamData where


iToO :: StreamData -> OutputStreamData
iToO (Hello i) = Hello' i
iToO (Goodbye s) = Goodbye' s


dataStream :: DataStream StreamData OutputStreamData
dataStream = Map iToO Identity

source :: Source StreamData
source = SourceKafkaTopic "test" (Goodbye . show)

sink :: Sink OutputStreamData
sink = StdOut $ B.concat . BL.toChunks . encode

pipeline :: Pipeline StreamData OutputStreamData
pipeline = Pipeline source dataStream sink

startTaskManager :: [NodeId] -> Process ()
startTaskManager peers =
  runTaskManager peers pipeline

doubleInt :: Int -> Int
doubleInt i = i * 2

doubleCl :: StreamData -> Int
doubleCl (Hello a) = a * 2
doubleCl (Goodbye _) = 0

numToZero :: Int -> [String]
numToZero n = fmap show [0..n]

remotable ['startTaskManager]

myRemoteTable :: RemoteTable
myRemoteTable =
  Main.__remoteTable
  initRemoteTable


main :: IO ()
main = do
  args <- getArgs

  case args of
    ["tm", host, port] -> do
      backend <- initializeBackend host port myRemoteTable
      startSlave backend
    ["jm", host, port] -> do
      backend <- initializeBackend host port myRemoteTable
      runJobManager backend pipeline $(mkClosure 'startTaskManager)
    _ -> return ()
