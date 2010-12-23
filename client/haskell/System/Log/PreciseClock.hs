module System.Log.PreciseClock (preciseTimeSpec, preciseTimestamp) where

import System.Posix.Clock
import Data.Time.Clock
import Data.Time.Calendar
import Data.Time.Format

import Control.Monad
import System.IO.Unsafe

(seedRealtime, seedMonotonic) = unsafePerformIO $ liftM2 (,) (getTime Realtime) (getTime Monotonic)

epochStart :: UTCTime
epochStart = UTCTime (toEnum 40587) 0

clock2utc :: TimeSpec -> UTCTime
clock2utc spec = toDiffTime spec `addUTCTime` epochStart

toDiffTime :: TimeSpec -> NominalDiffTime
toDiffTime (TimeSpec sec nsec) = fromIntegral sec + fromIntegral nsec / 1000000000

preciseTimeSpec :: IO TimeSpec
preciseTimeSpec = do
  TimeSpec ms mns <- getTime Monotonic
  let (TimeSpec rts0 rtns0) = seedRealtime
  let (TimeSpec ms0  mns0)  = seedMonotonic
  return $ normalize $ TimeSpec (rts0 + ms - ms0) (rtns0 + mns - mns0)

normalize ts@(TimeSpec s ns) | ns < 0 = normalize (TimeSpec (s-1) (ns+1000000000))
                             | ns > 1000000000 = normalize (TimeSpec (s+1) (ns-1000000000))
                             | otherwise = ts

preciseTimestamp :: IO UTCTime
preciseTimestamp = clock2utc `fmap` preciseTimeSpec
