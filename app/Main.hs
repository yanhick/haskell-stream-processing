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

dataStream :: DataStream StreamData String
{-dataStream = Map doubleCl $ Map doubleInt $ FlatMap numToZero $ Map read $ Map doubleInt Identity-}
dataStream = Map show $ Map ("message : " ++) Identity

source :: Source StreamData
source = SourceKafkaTopic "test" (Goodbye . show)

sink :: Sink String
sink = StdOut show

pipeline :: Pipeline StreamData String
pipeline = Pipeline source dataStream sink

runStream' :: [StreamData] -> Process ()
runStream' ds =
 runPipeline ds pipeline


doubleInt :: Int -> Int
doubleInt i = i * 2

doubleCl :: StreamData -> Int
doubleCl (Hello a) = a * 2
doubleCl (Goodbye _) = 0

numToZero :: Int -> [String]
numToZero n = fmap show [0..n]

remotable ['runStream']

myRemoteTable :: RemoteTable
myRemoteTable =
  Main.__remoteTable
  initRemoteTable


main :: IO ()
main = do
  args <- getArgs

  case args of
    ["master", host, port] ->
      DataStream.startPipeline host port myRemoteTable pipeline $(mkClosure 'runStream')
    ["slave", host, port] -> do
      backend <- initializeBackend host port myRemoteTable
      startSlave backend
    _ -> return ()
