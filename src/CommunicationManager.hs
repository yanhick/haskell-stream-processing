{-# LANGUAGE DeriveGeneric #-}

module CommunicationManager where

import           Control.Distributed.Process
import           Control.Monad
import           Data.Binary
import           Data.ByteString.Lazy
import           Data.Map                    as M
import           GHC.Generics

type ProcessIdMap = M.Map Int [ProcessId]

type CommunicationManagerPort = SendPort CommunicationManagerMessage

getMergedProcessIdMap ::
     [(Int, [ProcessId])] -> [(Int, [ProcessId])] -> ProcessIdMap
getMergedProcessIdMap local remote = M.fromListWith (++) (local ++ remote)

newtype TaskId =
  TaskId Int
  deriving (Generic, Show)

runTaskId :: TaskId -> Int
runTaskId (TaskId i) = i

instance Binary TaskId

data CommunicationManagerMessage
  = TaskResult TaskId
               ByteString
  | PartitionResult TaskId
                    ProcessId
                    ByteString
  deriving (Generic, Show)

instance Binary CommunicationManagerMessage

newtype IntermediateResult =
  IntermediateResult ByteString
  deriving (Generic)

instance Binary IntermediateResult

runCommunicationManager :: ProcessId -> Process ()
runCommunicationManager pid = do
  (sendPort, receivePort) <-
    newChan :: Process ( SendPort CommunicationManagerMessage
                       , ReceivePort CommunicationManagerMessage)
  send pid sendPort
  allProcessIds <- expect :: Process ProcessIdMap
  forever $ do
    msg <- receiveChan receivePort
    case msg of
      TaskResult (TaskId operatorId) value -> do
        let nextOperator = M.lookup (operatorId + 1) allProcessIds
        case nextOperator of
          (Just (x:_)) -> send x (IntermediateResult value)
          _            -> return ()
      PartitionResult _ pid' value -> send pid' (IntermediateResult value)
