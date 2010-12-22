{-# LANGUAGE CPP, MagicHash #-}
{-|
Messages are stored in TChan
1 thread performs calibration
1 'packer' thread takes messages from tchan and offloads them to sender thread(s).
1 'checking' thread keeps an eye on TChan size, initiates message dropping if necessary.
1 'sender' thread delivers the batch of messages to the server
-}
module System.Log.Greg (logMessage, withGregDo, defaultConfiguration) where

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

#if defined(__GLASGOW_HASKELL__) && !defined(__HADDOCK__)
import GHC.Base
import GHC.Word (Word32(..),Word16(..),Word64(..))

#if WORD_SIZE_IN_BITS < 64 && __GLASGOW_HASKELL__ >= 608
import GHC.Word (uncheckedShiftRL64#)
#endif
#else
import Data.Word
#endif

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
  forkIO $ forever (initiateCalibrationOnce st >> threadDelay (1000000*calibrationPeriodSec conf))

  -- Packer thread that offloads records to sender thread
  forkIO $ forever (packRecordsOnce         st >> threadDelay (1000*flushPeriodMs conf))

  -- Housekeeping thread that keeps queue size at check
  forkIO $ forever (checkQueueSize          st >> threadDelay (1000*flushPeriodMs conf))

  -- Sender thread
  forkIO $ forever (sendPacketOnce          st >> threadDelay (1000*flushPeriodMs conf))

  realMain

checkQueueSize :: GregState -> IO ()
checkQueueSize st = do
  currsize <- atomically $ readTVar (numRecords st)
  let maxrs = maxBufferedRecords (configuration st)
  droppingNow <- atomically $ readTVar (isDropping st)
  case (droppingNow, currsize > maxrs) of
    (True , True) -> putStrLn ("Still dropping (queue " ++ show currsize ++ ")")
    (False, True) -> do putStrLn ("Started to drop (queue " ++ show currsize ++ ")")
                        atomically $ writeTVar (isDropping st) True
    (True, False) -> do putStrLn ("Stopped dropping (queue " ++ show currsize ++ ")")
                        atomically $ writeTVar (isDropping st) False
    (False, False) -> return () -- everything is OK

packRecordsOnce :: GregState -> IO ()
packRecordsOnce st = do
  putStrLn $ "Packing: reading all messages ..."
  rs <- readAllMessages
  putStrLn $ "Packing: reading all messages done (" ++ show (length rs) ++ ")"
  unless (null rs) $ do
    putStrLn $ "Packing " ++ show (length rs) ++ " records"
    atomModTVar (numRecords st) (\x -> x - length rs) -- decrease queue length
    atomically $ do senderAccepted <- tryPutTMVar (packet st) rs -- putting messages in the outbox
                    unless senderAccepted retry 
    putStrLn "Packing done"
  where
    readAllMessages = do 
      empty <- atomically $ isEmptyTChan (records st)
      if empty then return []
        else do r <- atomically $ readTChan (records st)
                rest <- readAllMessages
                return (r:rest)

sendPacketOnce :: GregState -> IO ()
sendPacketOnce st = do
  rs <- atomically $ takeTMVar $ packet st
  let conf = configuration st
  putStrLn "Pushing records"
  bracket (connectTo (server conf) (PortNumber $ fromIntegral $ port conf)) hClose $ \hdl -> do
    putStrLn "Pushing records - connected"
    let msg = formatRecords (configuration st) rs
    putStrLn $ "Snapshotted " ++ show (length rs) ++ " records --> " ++ show (B.length msg) ++ " bytes"
    unsafeUseAsCStringLen msg $ \(ptr, len) -> hPutBuf hdl ptr len
    hFlush hdl
  putStrLn $ "Pushing records - done"

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
  putStrLn "Initiating calibration"
  let conf = configuration st
  bracket (connectTo (server conf) (PortNumber $ fromIntegral $ calibrationPort conf)) hClose $ \hdl -> do
    hSetBuffering hdl NoBuffering
    putStrLn "Calibration - connected"
    unsafeUseAsCString ourUuid $ \p -> hPutBuf hdl p 16
    allocaBytes 8 $ \pOurTimestamp -> do
      allocaBytes 8 $ \pTheirTimestamp -> do
        let whenM mp m = mp >>= \v -> when v m
            loop = whenM (hSkipBytes hdl 8 pTheirTimestamp) $ do
                    ts <- preciseTimeSpec
                    writeWord64le (toNanos64 ts) pOurTimestamp
                    hPutBuf hdl pOurTimestamp 8
                    -- putStrLn "Calibration - next loop iteration passed"
                    loop
        loop
  putStrLn "Calibration ended - sleeping"

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
toNanos64 (TimeSpec s ns) = fromIntegral (ns + 1000000000*s)

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


------------------------------------------------------------------------
-- Unchecked shifts

{-# INLINE shiftr_w16 #-}
shiftr_w16 :: Word16 -> Int -> Word16
{-# INLINE shiftr_w32 #-}
shiftr_w32 :: Word32 -> Int -> Word32
{-# INLINE shiftr_w64 #-}
shiftr_w64 :: Word64 -> Int -> Word64

#if defined(__GLASGOW_HASKELL__) && !defined(__HADDOCK__)
shiftr_w16 (W16# w) (I# i) = W16# (w `uncheckedShiftRL#`   i)
shiftr_w32 (W32# w) (I# i) = W32# (w `uncheckedShiftRL#`   i)

#if WORD_SIZE_IN_BITS < 64
shiftr_w64 (W64# w) (I# i) = W64# (w `uncheckedShiftRL64#` i)

#if __GLASGOW_HASKELL__ <= 606
-- Exported by GHC.Word in GHC 6.8 and higher
foreign import ccall unsafe "stg_uncheckedShiftRL64"
    uncheckedShiftRL64#     :: Word64# -> Int# -> Word64#
#endif

#else
shiftr_w64 (W64# w) (I# i) = W64# (w `uncheckedShiftRL#` i)
#endif

#else
shiftr_w16 = shiftR
shiftr_w32 = shiftR
shiftr_w64 = shiftR
#endif

-- | Write a 'Word64' in little endian format.
writeWord64le :: Word64 -> Ptr Word8 -> IO ()

#if WORD_SIZE_IN_BITS < 64
writeWord64le w p = do
    let b = fromIntegral (shiftr_w64 w 32) :: Word32
        a = fromIntegral w                 :: Word32
    poke (p)             (fromIntegral (a)               :: Word8)
    poke (p `plusPtr` 1) (fromIntegral (shiftr_w32 a  8) :: Word8)
    poke (p `plusPtr` 2) (fromIntegral (shiftr_w32 a 16) :: Word8)
    poke (p `plusPtr` 3) (fromIntegral (shiftr_w32 a 24) :: Word8)
    poke (p `plusPtr` 4) (fromIntegral (b)               :: Word8)
    poke (p `plusPtr` 5) (fromIntegral (shiftr_w32 b  8) :: Word8)
    poke (p `plusPtr` 6) (fromIntegral (shiftr_w32 b 16) :: Word8)
    poke (p `plusPtr` 7) (fromIntegral (shiftr_w32 b 24) :: Word8)
#else
writeWord64le w p = do
    poke p               (fromIntegral (w)               :: Word8)
    poke (p `plusPtr` 1) (fromIntegral (shiftr_w64 w  8) :: Word8)
    poke (p `plusPtr` 2) (fromIntegral (shiftr_w64 w 16) :: Word8)
    poke (p `plusPtr` 3) (fromIntegral (shiftr_w64 w 24) :: Word8)
    poke (p `plusPtr` 4) (fromIntegral (shiftr_w64 w 32) :: Word8)
    poke (p `plusPtr` 5) (fromIntegral (shiftr_w64 w 40) :: Word8)
    poke (p `plusPtr` 6) (fromIntegral (shiftr_w64 w 48) :: Word8)
    poke (p `plusPtr` 7) (fromIntegral (shiftr_w64 w 56) :: Word8)
#endif
{-# INLINE writeWord64le #-}

atomModTVar var f = atomically $ readTVar var >>= \val -> writeTVar var (f val)

main :: IO ()
main = withGregDo defaultConfiguration $ forever $ log "Hello" >> threadDelay 1000
