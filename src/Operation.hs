{-# LANGUAGE GADTs #-}

module Operation where

import           Data.Binary
import           Data.ByteString.Lazy

data Operation a b where
  Map :: (Binary a, Binary b) => (a -> b) -> Operation b c -> Operation a c
  Filter :: Binary a => (a -> Bool) -> Operation a b -> Operation a b
  FlatMap
    :: (Binary a, Binary b) => (a -> [b]) -> Operation b c -> Operation a c
  Partition :: Binary a => (a -> Int) -> Operation a b -> Operation a b
  Fold
    :: (Binary a, Binary b)
    => (a -> b -> a)
    -> a
    -> Operation a b
    -> Operation a b
  Identity :: Operation a a

data SerializingOperation
  = SerializingParDo SerializingParDo
  | SerializingPartition (ByteString -> Int)
  | SerializingFold ((ByteString, ByteString) -> ByteString)
                    ByteString

data SerializingParDo
  = SerializingMap (ByteString -> ByteString)
  | SerializingFilter (ByteString -> Bool)
  | SerializingFlatMap (ByteString -> Maybe [ByteString])

runSerializingParDo :: SerializingParDo -> ByteString -> Maybe [ByteString]
runSerializingParDo (SerializingMap f) x = Just [f x]
runSerializingParDo (SerializingFilter f) x =
  if f x
    then Just [x]
    else Nothing
runSerializingParDo (SerializingFlatMap f) x = f x

getSerializingOperations :: Operation a b -> [SerializingOperation]
getSerializingOperations (Map f cont) =
  (SerializingParDo $ SerializingMap (encode . f . decode)) :
  getSerializingOperations cont
getSerializingOperations (Filter f cont) =
  (SerializingParDo $ SerializingFilter (f . decode)) :
  getSerializingOperations cont
getSerializingOperations (FlatMap f cont) =
  (SerializingParDo $ SerializingFlatMap (return . fmap encode . f . decode)) :
  getSerializingOperations cont
getSerializingOperations Identity = []
getSerializingOperations (Partition f cont) =
  SerializingPartition (f . decode) : getSerializingOperations cont
getSerializingOperations (Fold f initValue cont) =
  SerializingFold
    (\(acc, v) -> encode $ f (decode acc) (decode v))
    (encode initValue) :
  getSerializingOperations cont
