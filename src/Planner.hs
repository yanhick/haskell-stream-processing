module Planner where

import           Operation
import           Pipeline

newtype OperatorId =
  OperatorId Int

data Operator a b
  = TransformationOperator SerializingOperation
  | SourceOperator (Source a)
  | SinkOperator (Sink b)

data RunnableOperator a b = RunnableOperator
  { operatorId :: OperatorId
  , operator   :: Operator a b
  }

type IndexedPlan a b = [RunnableOperator a b]

getIndexedPlan :: Pipeline a b -> IndexedPlan a b
getIndexedPlan (Pipeline source operation sink) =
  let operators =
        zip [0 ..] $
        SourceOperator source :
        (TransformationOperator <$> getSerializingOperations operation) ++
        [SinkOperator sink]
  in fmap
       (\(opId, op) ->
          RunnableOperator {operator = op, operatorId = OperatorId opId})
       operators

getOperatorIndices :: Pipeline a b -> [Int]
getOperatorIndices pipeline = fmap (\RunnableOperator { operatorId = OperatorId opId } -> opId) (getIndexedPlan pipeline)

getMergedIndexedPlan :: IndexedPlan a b -> [Int] -> IndexedPlan a b
getMergedIndexedPlan indexedPlan ids =
  filter
    (\RunnableOperator {operatorId = OperatorId opId} -> opId `elem` ids)
    indexedPlan
