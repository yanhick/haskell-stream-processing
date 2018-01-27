{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE DeriveGeneric      #-}
module TaskManager where

import           CommunicationManager
import           GHC.Generics
import           Control.Distributed.Process
import           Control.Monad
import           Data.Binary
import           Operation
import           Pipeline
import           Planner
import           Task

type OperatorsToRun = [Int]

data TaskManagerRunPlan =
  TaskManagerRunPlan OperatorsToRun
                     [(Int, [ProcessId])]
                     deriving Generic

instance Binary TaskManagerRunPlan where

runTaskManager ::
     (Binary a, Binary b, Show b)
  => TaskManagerRunPlan
  -> Pipeline a b
  -> Process ()
runTaskManager (TaskManagerRunPlan ids processIds) pipeline = do
  let plans = getMergedIndexedPlan (getIndexedPlan pipeline) ids
  selfPid <- getSelfPid
  communicationManagerPid <- spawnLocal $ runCommunicationManager selfPid
  sendPort <- expect :: Process CommunicationManagerPort
  nodes <- forM plans (startOperator sendPort)
  let allProcessIds = getMergedProcessIdMap nodes processIds
  send communicationManagerPid allProcessIds
  forM_ nodes (\(_, [nodePid]) -> send nodePid allProcessIds)

startOperator :: (Binary a, Binary b, Show b) => CommunicationManagerPort -> RunnableOperator a b -> Process (Int, [ProcessId])
startOperator sendPort RunnableOperator { operatorId = OperatorId operatorId, operator } =  do
    processId <- spawnLocal $ case operator of
      SinkOperator sink -> runSinkTask sink
      SourceOperator source -> runSourceTask (TaskId operatorId) sendPort source
      TransformationOperator transformationOperator -> startTransformationOperator operatorId transformationOperator sendPort
    return (operatorId, [processId])

startTransformationOperator :: Int -> SerializingOperation -> CommunicationManagerPort -> Process ()
startTransformationOperator operatorId operation sendPort = 
        case operation of
          SerializingParDo operation' ->
            runParDoTask (TaskId operatorId) sendPort operation'
          SerializingPartition f ->
            runPartitionTask (TaskId operatorId) sendPort f
          SerializingFold f initValue ->
            runFoldTask (TaskId operatorId) sendPort f initValue
