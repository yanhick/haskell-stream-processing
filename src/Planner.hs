module Planner where

import           Operation
import           Pipeline

data RunnableTask a b
  = OperationTask SerializingOperation
  | SourceTask (Source a)
  | SinkTask (Sink b)

type IndexedPlan = [(Int, SerializingOperation)]

type IndexedPlan' a b = [(Int, RunnableTask a b)]

getIndexedPlan' :: Pipeline a b -> IndexedPlan' a b
getIndexedPlan' (Pipeline source operation sink) =
  zip [0 ..] $
  SourceTask source :
  (OperationTask <$> getSerializingOperations operation) ++ [SinkTask sink]

getIndexedPlan :: Operation a b -> IndexedPlan
getIndexedPlan operation = zip [1 ..] (getSerializingOperations operation)

getMergedIndexedPlan :: IndexedPlan -> [Int] -> IndexedPlan
getMergedIndexedPlan indexedPlan ids =
  filter (\(taskId, _) -> taskId `elem` ids) indexedPlan
