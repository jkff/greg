module System.Log.BoundedBuffer (Buf(), makeBuf, push, poll, DropEvent(..)) where

import Control.Concurrent.MVar
import Control.Monad

data Buf a = Buf { state :: MVar (BufState a) }

data BufState a = BufState {contents :: [a], size :: Int, capacity :: Int, isDropping :: Bool, numDropped :: Int, dropHandler :: DropEvent -> IO ()}

data DropEvent = Started | Stopped { totalDropped :: Int } | Continued { droppedSoFar :: Int } deriving (Show)

makeBuf :: Int -> (DropEvent -> IO ()) -> IO (Buf a)
makeBuf cap dh = Buf `fmap` newMVar (BufState {contents = [], size = 0, capacity = cap, isDropping = False, numDropped = 0, dropHandler = dh})

push :: Buf a -> a -> IO ()
push (Buf vs) a = do
  s@(BufState contents size capacity isDropping numDropped dh) <- takeMVar vs
  if size < capacity
    then do
      when isDropping (dh (Stopped numDropped))
      putMVar vs (BufState (a:contents) (size+1) capacity False 0 dh)
    else do
      dh (if isDropping then Continued (numDropped+1) else Started)
      putMVar vs (BufState contents size capacity True (numDropped + 1) dh)

poll :: Buf a -> IO (Maybe a)
poll (Buf vs) = do
  s@(BufState contents n _ _ _ _) <- takeMVar vs
  case contents of
    []   -> putMVar vs s                         >> return Nothing
    a:as -> putMVar vs (s{contents=as,size=n-1}) >> return (Just a)

dumpBuf :: (Show a) => Buf a -> IO ()
dumpBuf (Buf vs) = do
  BufState contents size cap isDropping numDropped _ <- readMVar vs
  putStrLn . show $ (contents, size, cap, isDropping, numDropped)
