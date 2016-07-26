{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE BangPatterns #-}

module Pipes.Aws.S3.Download.Retry
    ( fromS3WithRetries
      -- ** Deciding when to retry
    , RetryPolicy(..)
    , retryNTimes
    ) where

import Data.IORef

import qualified Data.ByteString as BS

import Pipes
import Pipes.Safe
import qualified Pipes.Prelude as PP

import Pipes.Aws.S3.Types
import Pipes.Aws.S3.Download

-- | How many times to attempt an object download before giving up.
data RetryPolicy m = forall s. RetryIf s (s -> SomeException -> m (s, Bool))

-- | Always retry.
instance Applicative m => Monoid (RetryPolicy m) where
    mempty = RetryIf () (\s _ -> pure (s, True))
    RetryIf sx0 px `mappend` RetryIf sy0 py =
        RetryIf (sx0, sy0) (\(sx, sy) exc -> merge <$> px sx exc <*> py sy exc)
      where
        merge (sx1, againX) (sy1, againY) = ((sx1, sy1), againX && againY)

-- | Retry a download no more than @n@ times.
retryNTimes :: Applicative m => Int -> RetryPolicy m
retryNTimes n0 = RetryIf n0 shouldRetry
  where
    shouldRetry !n _ = pure (n-1, n > 0)

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
            (retryAcc', again) <- lift $ shouldRetry retryAcc exc
            if again
              then go retryAcc' offsetVar
              else fail $ "Failed to download "++show bucket++" "++show object

        trackOffset bs = do
            liftIO $ modifyIORef' offsetVar (+ BS.length bs)
            return bs
