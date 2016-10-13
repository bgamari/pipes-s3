{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE BangPatterns #-}

-- | @pipes@ 'Producer's from downloading data from AWS S3 objects, with
-- retry-based failure handling.
module Pipes.Aws.S3.Download.Retry
    ( fromS3WithRetries
      -- * Deciding when to retry
    , RetryPolicy(..)
    , retryNTimes
    , retryIfException
      -- * Diagnostics
    , warnOnRetry
    ) where

import Control.Monad (when)
import Data.IORef
import System.IO

import qualified Data.ByteString as BS

import Pipes
import Pipes.Safe
import qualified Pipes.Prelude as PP

import Pipes.Aws.S3.Types
import Pipes.Aws.S3.Download

-- | How many times to attempt an object download before giving up.
data RetryPolicy m = forall s. RetryIf s (Bucket -> Object -> s -> SomeException -> m (s, Bool))

-- | @mempty@ will always retry. @mappend@ will retry only if both
-- 'RetryPolicy's say to retry.
instance Applicative m => Monoid (RetryPolicy m) where
    mempty = RetryIf () (\_ _ s _ -> pure (s, True))
    RetryIf sx0 px `mappend` RetryIf sy0 py =
        RetryIf (sx0, sy0) (\bucket object (sx, sy) exc -> merge <$> px bucket object sx exc <*> py bucket object sy exc)
      where
        merge (sx1, againX) (sy1, againY) = ((sx1, sy1), againX && againY)

-- | Retry a download no more than @n@ times.
retryNTimes :: Applicative m => Int -> RetryPolicy m
retryNTimes n0 = RetryIf n0 shouldRetry
  where
    shouldRetry _ _ !n _ = pure (n-1, n > 0)

-- | Retry if the exception thrown satisfies the given predicate.
retryIfException :: Applicative m => (SomeException -> Bool) -> RetryPolicy m
retryIfException predicate = RetryIf () (\_ _ s exc -> pure (s, predicate exc))

-- | Modify a 'RetryPolicy' to print a warning on stderr when a retry is
-- attempted.
warnOnRetry :: MonadIO m => RetryPolicy m -> RetryPolicy m
warnOnRetry (RetryIf s0 action) =
    RetryIf (0::Int, s0) $ \bucket object (!n,s) exc -> do
        (s', shouldRetry) <- action bucket object s exc
        let msg = show bucket ++ "/" ++ show object ++ ": Retry attempt " ++ show n ++ " failed: " ++ show exc
        when shouldRetry $ liftIO $ hPutStrLn stderr msg
        return ((n+1, s'), shouldRetry)

-- | Download an object from S3, retrying a finite number of times on failure.
fromS3WithRetries :: forall m. (MonadSafe m)
                  => RetryPolicy m -> Bucket -> Object
                  -> Producer BS.ByteString m ()
fromS3WithRetries (RetryIf retryAcc0 shouldRetry) bucket object = do
    offsetVar <- liftIO $ newIORef 0
    go retryAcc0 offsetVar
  where
    --go :: s -> IORef Int -> Producer BS.ByteString m ()
    go retryAcc offsetVar = do
        offset <- liftIO $ readIORef offsetVar
        handleAll retry $
            fromS3 bucket object (Just $ ContentRange offset maxBound) >-> PP.mapM trackOffset
      where
        retry exc = do
            (retryAcc', again) <- lift $ shouldRetry bucket object retryAcc exc
            if again
              then go retryAcc' offsetVar
              else fail $ "Failed to download "++show bucket++" "++show object

        trackOffset bs = do
            liftIO $ modifyIORef' offsetVar (+ BS.length bs)
            return bs
