{-# LANGUAGE CPP, MagicHash #-}
module System.Log.Greg (log) where

{-
Idea:
Messages are stored in Chan/TChan
1 thread performs calibration and controls socket
1 'keeper' thread takes messages from tchan and offloads them to sender thread(s). Also, keeps tchan size in check, and controls socket
1 'sender' delivers the batch of messages to the server
-}

import Prelude hiding (log, getContents)

import System.Log.BoundedBuffer
import System.Log.PreciseClock
import System.Posix.Clock

import Data.ByteString.Unsafe
import qualified Data.ByteString.Char8 as B
import qualified Data.ByteString.Lazy.Char8 as L
import Data.Binary
import Data.Binary.Put

import Network.Socket
import Network.HostName
import Network.Socket.ByteString

import Data.UUID
import System.UUID.V4

import Data.Time.Clock

import System.IO
import Foreign
import Foreign.Ptr
import Foreign.Marshal.Alloc

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
import Control.Concurrent.MVar
import Control.Concurrent.Chan
import Control.Concurrent.STM
import System.IO.Unsafe
import Control.Monad

data Record = Record {
        timestamp :: TimeSpec,
        message :: B.ByteString
    }

data GregState = GregState { 
        configuration :: Configuration,
        records :: TChan Record,
        packet :: TMVar [Record]
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

{- TODO:
makeBuf (maxBufferedRecords conf) 
                     (\de -> case de of
                        Started     -> putStrLn "Started dropping"
                        Stopped   t -> putStrLn ("Stopped dropping, dropped " ++ show t)
                        Continued t -> putStrLn ("Still dropping, dropped " ++ show t))
  -}
withGregDo :: Configuration -> IO () -> IO ()
withGregDo conf main = withSocketsDo $ do
  st' <- atomically $ readTVar state
  let st = st' {configuration = conf}
  let withAdr host port f = do {
     ai <- getAddrInfo (Just AddrInfo {
        addrFlags=[AI_ADDRCONFIG,AI_NUMERICSERV], 
        addrFamily=AF_INET, 
        addrSocketType=Stream, 
        addrProtocol=defaultProtocol}) (Just host) (Just (show port))
   ; case ai of {
       []   -> putStrLn $ "Cannot resolve " ++ host
     ; a:as -> do {
          when (not (null as)) $ putStrLn $ "Ignored other addresses of " ++ host ++ ": " ++ show as
        ; let adr = addrAddress a
        ; f adr
        ; return ()
       }
     }
  }

  atomically $ writeTVar state st
  -- Packer thread
  withAdr (server conf) (port conf) $ \adr -> 
    forkIO $ forever (packRecordsOnce         st >> threadDelay (1000*flushPeriodMs conf))

  -- Calibration thread
  withAdr (server conf) (calibrationPort conf) $ \adr -> 
    forkIO $ forever (initiateCalibrationOnce st adr >> threadDelay (1000000*calibrationPeriodSec conf))

  -- Sender thread
  withAdr (server conf) (port conf) $ \adr -> 
    forkIO $ forever (pushRecordsOnce         st adr >> threadDelay (1000*flushPeriodMs conf))

  main

packRecordsOnce :: GregState -> IO ()
packRecordsOnce st = do
  atomically $ do rs <- readAll
                  if null rs then retry
                    else putTMVar (packet st) rs
  where
    readAll = do empty <- isEmptyTChan (records st)
                 if empty then return []
                   else do r <- readTChan (records st)
                           rest <- readAll
                           return (r:rest)

pushRecordsOnce :: GregState -> SockAddr -> IO ()
pushRecordsOnce st adr = do
  rs <- atomically $ takeTMVar $ packet st
  putStrLn "Pushing records"
  bracket (socket AF_INET Stream defaultProtocol) sClose $ \sock -> do
    putStrLn "Pushing records - connecting..."
    connect sock adr
    putStrLn "Pushing records - connected"
    let msg = formatRecords (configuration st) rs
    putStrLn $ "Snapshotted " ++ show (length rs) ++ " records --> " ++ show (B.length msg) ++ " bytes"
    sendAll sock msg

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

initiateCalibrationOnce :: GregState -> SockAddr -> IO ()
initiateCalibrationOnce st adr = do
  putStrLn "Initiating calibration"
  bracket (socket AF_INET Stream defaultProtocol) sClose $ \sock -> do
    setSocketOption sock NoDelay 1
    putStrLn "Calibration - connecting..."
    connect sock adr
    putStrLn "Calibration - connected"
    bracket (socketToHandle sock ReadWriteMode) hClose $ \h -> do
      putStrLn "Calibration - converted socket to handle"
      unsafeUseAsCString ourUuid $ \p -> hPutBuf h p 16
      allocaBytes 8 $ \pOurTimestamp -> do
      allocaBytes 8 $ \pTheirTimestamp -> do
      let whenM mp m = mp >>= \v -> when v m
      forever $ do
        whenM (hSkipBytes h 8 pTheirTimestamp) $ do {
        ; ts <- preciseTimeSpec
        ; writeWord64le (toNanos64 ts) pOurTimestamp
        ; hPutBuf h pOurTimestamp 8
        ; putStrLn "Calibration - next loop iteration passed"
        }

state :: TVar GregState
state = unsafePerformIO $ do rs <- newTChanIO
                             pkt <- newEmptyTMVarIO
                             newTVarIO $ GregState defaultConfiguration rs pkt

log :: String -> IO ()
log s = do
  t <- preciseTimeSpec
  atomically $ do 
    st <- readTVar state
    writeTChan (records st) (Record {timestamp = t, message = B.pack s})

--------------------------------------------------------------------------
-- Utilities

toNanos64 :: TimeSpec -> Word64
toNanos64 (TimeSpec s ns) = fromIntegral (ns + 1000000000*s)

hSkipBytes :: Handle -> Int -> Ptr a -> IO Bool
hSkipBytes _ 0 _ = return True
hSkipBytes h n p = do
  skipped <- hGetBuf h p n 
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




main = withGregDo defaultConfiguration $ forever $ log "Hello" >> threadDelay 1000
