module Planner where
import Operation

type IndexedPlan = [(Int, SerializingOperation)]

getIndexedPlan :: Operation a b -> IndexedPlan
getIndexedPlan operation = zip [1 ..] (getSerializingOperations operation)

getMergedIndexedPlan :: IndexedPlan -> [Int] -> IndexedPlan
getMergedIndexedPlan indexedPlan ids =
  filter (\(taskId, _) -> taskId `elem` ids) indexedPlan
