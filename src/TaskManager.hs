module TaskManager where

import Data.Binary
import Control.Monad
import Control.Distributed.Process
import Pipeline
import CommunicationManager
import Task
import Operation
import Planner

type OperatorsToRun = [Int]

data TaskManagerRunPlan =
  TaskManagerRunPlan OperatorsToRun
                     [(Int, [ProcessId])]

runTaskManager :: (Binary a, Binary b, Show b) => TaskManagerRunPlan -> Pipeline a b -> Process ()
runTaskManager (TaskManagerRunPlan ids processIds) (Pipeline source ds sink) = do
  let plans = getMergedIndexedPlan (getIndexedPlan ds) ids
  selfPid <- getSelfPid
  communicationManagerPid <- spawnLocal $ runCommunicationManager selfPid
  sendPort <- expect :: Process CommunicationManagerPort
  nodes <-
    forM plans $ \(operatorId, ds') -> do
      processId <-
        spawnLocal $ case ds' of
          SerializingParDo ds'' -> runParDoTask (TaskId operatorId) sendPort ds''
          SerializingPartition f -> runPartitionTask (TaskId operatorId) sendPort f
          SerializingFold f initValue -> runFoldTask (TaskId operatorId) sendPort f initValue
      return (operatorId, [processId])
  sourceNode <-
    spawnLocal $ runSourceTask (TaskId 0) sendPort source
  sinkNode <- spawnLocal $ runSinkTask sink
  let sinkNodeIndex = length (getIndexedPlan ds) + 1
  let allProcessIds =
        getMergedProcessIdMap
          ((0, [sourceNode]) : (sinkNodeIndex, [sinkNode]) : nodes)
          processIds
  send communicationManagerPid allProcessIds
  forM_ nodes (\(_, [nodePid]) -> send nodePid allProcessIds)
