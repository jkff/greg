{-# LANGUAGE CPP #-}
{-|
Messages are stored in TChan
1 thread performs calibration
1 'packer' thread takes messages from tchan and offloads them to sender thread(s).
1 'checking' thread keeps an eye on TChan size, initiates message dropping if necessary.
1 'sender' thread delivers the batch of messages to the server
-}
module System.Log.Greg (
    Configuration(..)
   ,logMessage
   ,withGregDo
   ,defaultConfiguration
   ) where

import System.Log.PreciseClock
import System.Posix.Clock

import Data.ByteString.Unsafe
import qualified Data.ByteString.Char8 as B
import qualified Data.ByteString.Lazy.Char8 as L
import Data.Binary
import Data.Binary.Put

import Network
import Network.HostName (getHostName)

import System.UUID.V4

import System.IO
import Foreign

import Control.Exception
import Control.Concurrent
import Control.Concurrent.STM
import Control.Monad

data Record = Record {
        timestamp :: TimeSpec,
        message :: B.ByteString
    }

data GregState = GregState { 
        configuration :: Configuration,
        records :: TChan Record, -- FIFO for queued Records
        numRecords :: TVar Int, -- How many records are in FIFO
        isDropping :: TVar Bool, -- True is we are not adding records to the FIFO since there are more than 'maxBufferedRecords' of them
        packet :: TMVar [Record] -- Block of records we are currently trying to send
    }

data Configuration = Configuration {
        server :: String,
        port :: Int,
        calibrationPort :: Int,
        flushPeriodMs :: Int,
        clientId :: String,
        maxBufferedRecords :: Int,
        useCompression :: Bool,
        calibrationPeriodSec :: Int
    }

hostname, ourUuid :: B.ByteString
hostname = B.pack $ unsafePerformIO getHostName
ourUuid = repack . runPut . put $ unsafePerformIO uuid

defaultConfiguration :: Configuration
defaultConfiguration = Configuration {
        server = "localhost",
        port = 5676,
        calibrationPort = 5677,
        flushPeriodMs = 1000,
        clientId = "unknown",
        maxBufferedRecords = 100000,
        useCompression = True,
        calibrationPeriodSec = 10
    }

withGregDo :: Configuration -> IO () -> IO ()
withGregDo conf realMain = withSocketsDo $ do
  st <- atomically $ do st <- readTVar state
                        let st' = st{configuration = conf}
                        writeTVar state $ st'
                        return st'
  -- Calibration thread
  calTID   <- forkIO $ forever (initiateCalibrationOnce st >> threadDelay (1000000*calibrationPeriodSec conf))

  -- Packer thread that offloads records to sender thread
  packTID  <- forkIO $ forever (packRecordsOnce         st >> threadDelay (1000*flushPeriodMs conf))

  -- Housekeeping thread that keeps queue size at check
  checkTID <- forkIO $ forever (checkQueueSize          st >> threadDelay (1000*flushPeriodMs conf))

  -- Sender thread
  sendTID  <- forkIO $ forever (sendPacketOnce          st >> threadDelay (1000*flushPeriodMs conf))

  realMain
  putStrLnT "Flushing remaining messages"
  
  -- Shutdown. For now, just wait untill all messages are out of the queue
  -- 1. Stop reception of new messages
  killThread checkTID
  atomically $ writeTVar (isDropping st) True
  -- 2. Wait until all messages are sent
  let waitFlush = do
        numrs <- atomically $ readTVar (numRecords st)
        unless (numrs == 0) $ threadDelay (1000*flushPeriodMs conf) >> waitFlush
  waitFlush
  killThread packTID
  atomically $ putTMVar (packet st) []
  let waitSend = do
        sent <- atomically $ isEmptyTMVar (packet st)
        unless sent $ threadDelay (1000*flushPeriodMs conf) >> waitSend
  waitSend
  killThread sendTID
  killThread calTID
  putStrLnT "Shutdown finished."

checkQueueSize :: GregState -> IO ()
checkQueueSize st = do
  currsize <- atomically $ readTVar (numRecords st)
  let maxrs = maxBufferedRecords (configuration st)
  droppingNow <- atomically $ readTVar (isDropping st)
  case (droppingNow, currsize > maxrs) of
    (True , True) -> putStrLnT ("Still dropping (queue " ++ show currsize ++ ")")
    (False, True) -> do putStrLnT ("Started to drop (queue " ++ show currsize ++ ")")
                        atomically $ writeTVar (isDropping st) True
    (True, False) -> do putStrLnT ("Stopped dropping (queue " ++ show currsize ++ ")")
                        atomically $ writeTVar (isDropping st) False
    (False, False) -> return () -- everything is OK

packRecordsOnce :: GregState -> IO ()
packRecordsOnce st = do
  putStrLnT $ "Packing: reading all messages ..."
  rs <- readAtMost (10000::Int) -- Mandated by protocol
  putStrLnT $ "Packing: reading all messages done (" ++ show (length rs) ++ ")"
  unless (null rs) $ do
    putStrLnT $ "Packing " ++ show (length rs) ++ " records"
    atomModTVar (numRecords st) (\x -> x - length rs) -- decrease queue length
    atomically $ do senderAccepted <- tryPutTMVar (packet st) rs -- putting messages in the outbox
                    unless senderAccepted retry 
    putStrLnT "Packing done"
  where
    readAtMost 0 = return []
    readAtMost n = do 
      empty <- atomically $ isEmptyTChan (records st)
      if empty then return []
        else do r <- atomically $ readTChan (records st)
                rest <- readAtMost (n-1)
                return (r:rest)

sendPacketOnce :: GregState -> IO ()
sendPacketOnce st = do
  rs <- atomically $ takeTMVar $ packet st
  unless (null rs) $ do
    let conf = configuration st
    putStrLnT "Pushing records"
    bracket (connectTo (server conf) (PortNumber $ fromIntegral $ port conf)) hClose $ \hdl -> do
      putStrLnT "Pushing records - connected"
      let msg = formatRecords (configuration st) rs
      putStrLnT $ "Snapshotted " ++ show (length rs) ++ " records --> " ++ show (B.length msg) ++ " bytes"
      unsafeUseAsCStringLen msg $ \(ptr, len) -> hPutBuf hdl ptr len
      hFlush hdl
    putStrLnT $ "Pushing records - done"

formatRecords :: Configuration -> [Record] -> B.ByteString
formatRecords conf records = repack . runPut $ do
  putByteString ourUuid
  putWord8 0
  putWord32le (fromIntegral $ length $ clientId conf)
  putByteString (B.pack $ clientId conf)
  mapM_ putRecord records
  putWord32le 0

putRecord :: Record -> Put
putRecord r = do
  putWord32le 1
  putWord64le (toNanos64 (timestamp r))
  putWord32le (fromIntegral $ B.length hostname)
  putByteString hostname
  putWord32le (fromIntegral $ B.length (message r))
  putByteString (message r)

initiateCalibrationOnce :: GregState -> IO ()
initiateCalibrationOnce st = do
  putStrLnT "Initiating calibration"
  let conf = configuration st
  bracket (connectTo (server conf) (PortNumber $ fromIntegral $ calibrationPort conf)) hClose $ \hdl -> do
    hSetBuffering hdl NoBuffering
    putStrLnT "Calibration - connected"
    unsafeUseAsCString ourUuid $ \p -> hPutBuf hdl p 16
    allocaBytes 8 $ \pTheirTimestamp -> do
      let whenM mp m = mp >>= \v -> when v m
          loop = whenM (hSkipBytes hdl 8 pTheirTimestamp) $ do
                   ts <- preciseTimeSpec
                   let pOurTimestamp = repack $ runPut $ putWord64le (toNanos64 ts)
                   unsafeUseAsCString pOurTimestamp $ \ptr -> hPutBuf hdl ptr 8
                   -- putStrLnT "Calibration - next loop iteration passed"
                   loop
      loop
  putStrLnT "Calibration ended - sleeping"

state :: TVar GregState
state = unsafePerformIO $ do rs <- newTChanIO
                             numrs <- newTVarIO 0
                             dropping <- newTVarIO False
                             pkt <- newEmptyTMVarIO
                             newTVarIO $ GregState defaultConfiguration rs numrs dropping pkt

logMessage :: String -> IO ()
logMessage s = do
  t <- preciseTimeSpec
  st <- atomically $ readTVar state
  shouldDrop <- atomically $ readTVar (isDropping st)
  unless shouldDrop $ do
    atomically $ writeTChan (records st) (Record {timestamp = t, message = B.pack s})
    atomModTVar (numRecords st) (+1)

--------------------------------------------------------------------------
-- Utilities

toNanos64 :: TimeSpec -> Word64
toNanos64 (TimeSpec s ns) = fromIntegral ns + 1000000000*fromIntegral s

hSkipBytes :: Handle -> Int -> Ptr a -> IO Bool
hSkipBytes _ 0 _ = return True
hSkipBytes h n p = do
  closed <- hIsEOF h
  if closed 
    then return False
    else do skipped <- hGetBuf h p n 
            if skipped < 0 
              then return False 
              else hSkipBytes h (n-skipped) p

repack :: L.ByteString -> B.ByteString
repack = B.concat . L.toChunks

atomModTVar :: TVar a -> (a -> a) -> IO ()
atomModTVar var f = atomically $ readTVar var >>= \val -> writeTVar var (f val)

putStrLnT :: String -> IO ()
#ifdef DEBUG
putStrLnT = putStrLn
#else
putStrLnT _ = return ()
#endif

#ifdef SELFTEST
main :: IO ()
main = withGregDo defaultConfiguration $ forever $ logMessage "Hello" -- >> threadDelay 1000
#endif
