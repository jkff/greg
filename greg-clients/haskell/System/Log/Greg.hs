{-# LANGUAGE CPP #-}
-------------------------------------------------------------------------------
-- |
-- Copyright        :   (c) 2010 Eugene Kirpichov, Dmitry Astapov
-- License          :   BSD3
-- 
-- Maintainer       :   Eugene Kirpichov <ekirpichov@gmail.com>, 
--                      Dmitry Astapov <dastapov@gmail.com>
-- Stability        :   experimental
-- Portability      :   GHC only (STM, GHC.Conc for unsafeIOToSTM)
-- 
-- This module provides a binding to the greg distributed logger,
-- which provides a high-precision global time axis and is very performant.
-- 
-- See project home page at <http://code.google.com/p/greg> for an explanation
-- of how to use the server, the features, motivation and design.
--

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

#ifdef DEBUG
import Debug.Trace
#endif

import qualified Control.Exception as E
import Control.Concurrent
import Control.Concurrent.STM
import GHC.Conc
import Control.Monad

{-
Messages are stored in TChan
1 thread performs calibration
1 'packer' thread takes messages from tchan and offloads them to sender thread(s).
1 'checking' thread keeps an eye on TChan size, initiates message dropping if necessary.
1 'sender' thread delivers the batch of messages to the server
-}

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

-- | Client configuration.
-- You probably only need to change @server@.
data Configuration = Configuration {
        server                  :: String   -- ^ Server hostname (default @localhost@)
       ,port                    :: Int      -- ^ Message port (default @5676@)
       ,calibrationPort         :: Int      -- ^ Calibration port (default @5677@)
       ,flushPeriodMs           :: Int      -- ^ How often to send message batches to server 
                                            --   (default @1000@)
       ,clientId                :: String   -- ^ Arbitrary identifier, will show up in logs.
                                            --   For example, @\"DataService\"@ 
                                            --   (default @\"unknown\"@)
       ,maxBufferedRecords      :: Int      -- ^ How many records to store between flushes
                                            --   (more will be dropped) (default @100000@)
       ,useCompression          :: Bool     -- ^ Whether to use gzip compression 
                                            --   (default @False@, @True@ is unsupported)
       ,calibrationPeriodSec    :: Int      -- ^ How often to initiate calibration exchanges
                                            --   (default @10@)
    }

hostname, ourUuid :: B.ByteString
hostname = B.pack $ unsafePerformIO getHostName
ourUuid = repack . runPut . put $ unsafePerformIO uuid

-- | The default configuration, suitable for most needs.
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

-- | Perform an IO action with logging (will wait for all messages to flush).
withGregDo :: Configuration -> IO () -> IO ()
withGregDo conf realMain = withSocketsDo $ do
  st <- atomically $ do st <- readTVar state
                        let st' = st{configuration = conf}
                        writeTVar state $ st'
                        return st'
  
  let everyMs ms action = forkIO $ forever (action >> threadDelay (1000 * ms))
  let safely action label = action `E.catch` \e -> putStrLnT ("Error in " ++ label ++ ": " ++ show (e::E.SomeException))
  let safelyEveryMs ms action label = everyMs ms (safely action label)

  -- Packer thread offloads records to sender thread
  -- Housekeeping thread keeps queue size at check
  calTID   <- safelyEveryMs (1000*calibrationPeriodSec conf) (initiateCalibrationOnce st) "calibrator"
  packTID  <- safelyEveryMs (            flushPeriodMs conf) (packRecordsOnce         st) "packer"
  checkTID <- safelyEveryMs (            flushPeriodMs conf) (checkQueueSize          st) "queue size checker"
  sendTID  <- safelyEveryMs (            flushPeriodMs conf) (sendPacketOnce          st) "sender"

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
packRecordsOnce st = atomically $ do
  putStrLnT $ "Packing: reading all messages ..."
  rs <- readAtMost (10000::Int) -- Mandated by protocol
  putStrLnT $ "Packing: reading all messages done (" ++ show (length rs) ++ ")"
  unless (null rs) $ do
    putStrLnT $ "Packing " ++ show (length rs) ++ " records"
    atomModTVar (numRecords st) (\x -> x - length rs) -- decrease queue length
    senderAccepted <- tryPutTMVar (packet st) rs -- putting messages in the outbox
    unless senderAccepted retry 
    putStrLnT "Packing done"
  where
    readAtMost 0 = return []
    readAtMost n = do 
      empty <- isEmptyTChan (records st)
      if empty then return []
        else do r <- readTChan (records st)
                rest <- readAtMost (n-1)
                return (r:rest)

sendPacketOnce :: GregState -> IO ()
sendPacketOnce st = atomically $ withWarning "Failed to pack/send records" $ do
  rs <- takeTMVar $ packet st
  unless (null rs) $ do
    let conf = configuration st
    putStrLnT "Pushing records"
    unsafeIOToSTM $ E.bracket (connectTo (server conf) (PortNumber $ fromIntegral $ port conf)) hClose $ \hdl -> do
      putStrLnT "Pushing records - connected"
      let msg = formatRecords (configuration st) rs
      putStrLnT $ "Snapshotted " ++ show (length rs) ++ " records --> " ++ show (B.length msg) ++ " bytes"
      unsafeUseAsCStringLen msg $ \(ptr, len) -> hPutBuf hdl ptr len
      hFlush hdl
    putStrLnT $ "Pushing records - done"
  where
    withWarning s t = (t `catchSTM` (\e -> putStrLnT (s ++ ": " ++ show (e::E.SomeException)) >> check False)) `orElse` return ()
   

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
  E.bracket (connectTo (server conf) (PortNumber $ fromIntegral $ calibrationPort conf)) hClose $ \hdl -> do
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
-- | Log a message. The message will show up in server's output
-- annotated with a global timestamp (client's clock offset does 
-- not matter).
logMessage :: String -> IO ()
logMessage s = do
  t <- preciseTimeSpec
  st <- atomically $ readTVar state
  shouldDrop <- atomically $ readTVar (isDropping st)
  unless shouldDrop $ atomically $ do
    writeTChan (records st) (Record {timestamp = t, message = B.pack s})
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

atomModTVar :: TVar a -> (a -> a) -> STM ()
atomModTVar var f = readTVar var >>= \val -> writeTVar var (f val)

putStrLnT :: (Monad m) => String -> m ()
#ifdef DEBUG
putStrLnT s = trace s $ return ()
#else
putStrLnT _ = return ()
#endif

#ifdef DEBUG
testFlood :: IO ()
testFlood = withGregDo defaultConfiguration $ forever $ logMessage "Hello" -- >> threadDelay 1000

testSequence :: IO ()
testSequence = withGregDo defaultConfiguration $ mapM_ (\x -> logMessage (show x) >> threadDelay 100000) [1..]
#endif
