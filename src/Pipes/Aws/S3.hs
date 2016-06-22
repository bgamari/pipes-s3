{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- | A simple streaming interface to the AWS S3 storage service.
module Pipes.Aws.S3
   ( Bucket(..)
   , Object(..)
     -- * Downloading
     -- | These may fail with either an 'S3DownloadError' or a 'Aws.S3Error'.
   , fromS3
   , fromS3'
   , fromS3WithManager

     -- * Uploading
     -- | These internally use the S3 multi-part upload interface to achieve
     -- streaming upload behavior.
     --
     -- In the case of failure one of two exceptions will be thrown,
     --
     --   - 'EmptyS3UploadError': In the event that the 'Producer' fails to
     --     produce any content to upload
     --
     --   - 'FailedUploadError': In any other case.
     --
     -- The 'FailedUploadError' exception carries the 'UploadId' of the failed
     -- upload as well as the inner exception. Note that while the library makes
     -- an attempt to clean up the parts of the partial upload, there may still
     -- be remnants due to limitations in the @aws@ library.
     --
   , ChunkSize
   , toS3
   , toS3'
   , toS3WithManager

     -- * Error handling
   , S3DownloadError(..)
   , EmptyS3UploadError(..)
   , FailedUploadError(..)
   , UploadId(..)
   ) where

import Control.Monad (unless, when)
import Control.Exception (Exception)
import Data.String (IsString)

import qualified Data.ByteString as BS
import           Data.ByteString (ByteString)
import qualified Data.Text as T

import Pipes
import Pipes.Safe
import qualified Pipes.Prelude as PP
import qualified Pipes.ByteString as PBS
import Control.Monad.Trans.Resource
import Network.HTTP.Types
import Network.HTTP.Client
import Network.HTTP.Client.TLS
import qualified Aws
import qualified Aws.Core as Aws
import qualified Aws.S3 as S3

-- | An AWS S3 bucket name
newtype Bucket = Bucket T.Text
               deriving (Eq, Ord, Show, Read, IsString)

-- | An AWS S3 object name
newtype Object = Object T.Text
               deriving (Eq, Ord, Show, Read, IsString)

-- | Thrown when an unknown status code is returned from an S3 download request.
data S3DownloadError = S3DownloadError Bucket Object Status
                     deriving (Show)

instance Exception S3DownloadError

-- | Thrown when an upload with no data is attempted.
data EmptyS3UploadError = EmptyS3UploadError Bucket Object
                        deriving (Show)

instance Exception EmptyS3UploadError

-- | An identifier representing an active upload.
newtype UploadId = UploadId T.Text
                 deriving (Show, Eq, Ord)

-- | Thrown when an error occurs during an upload.
data FailedUploadError = FailedUploadError { failedUploadBucket    :: Bucket
                                           , failedUploadObject    :: Object
                                           , failedUploadException :: SomeException
                                           , failedUploadId        :: UploadId
                                           }
                       deriving (Show)

instance Exception FailedUploadError

-- | Download an object from S3
--
-- Note that this makes no attempt at reusing a 'Manager' and therefore may not
-- be very efficient for many small requests. See 'fromS3WithManager' for more
-- control over the 'Manager' used.
fromS3 :: MonadSafe m
       => Bucket -> Object
       -> Producer BS.ByteString m ()
fromS3 bucket object = do
    cfg <- liftIO Aws.baseConfiguration
    fromS3' cfg Aws.defServiceConfig bucket object

-- | Download an object from S3 explicitly specifying an @aws@ 'Aws.Configuration',
-- which provides credentials and logging configuration.
--
-- Note that this makes no attempt at reusing a 'Manager' and therefore may not
-- be very efficient for many small requests. See 'fromS3WithManager' for more
-- control over the 'Manager' used.
fromS3' :: MonadSafe m
        => Aws.Configuration                    -- ^ e.g. from 'Aws.baseConfiguration'
        -> S3.S3Configuration Aws.NormalQuery   -- ^ e.g. 'Aws.defServiceConfig'
        -> Bucket -> Object
        -> Producer BS.ByteString m ()
fromS3' cfg s3cfg bucket object = do
    mgr <- liftIO $ newManager tlsManagerSettings
    fromS3WithManager mgr cfg s3cfg bucket object

-- | Download an object from S3 explicitly specifying an @http-client@ 'Manager'
-- and @aws@ 'Aws.Configuration' (which provides credentials and logging
-- configuration).
--
-- This can be more efficient when submitting many small requests as it allows
-- re-use of the 'Manager' across requests. Note that the 'Manager' provided
-- must support TLS; such a manager can be created with
--
-- @
-- 'newManager' 'HTTP.Client.TLS.tlsManagerSettings'
-- @
fromS3WithManager
        :: MonadSafe m
        => Manager
        -> Aws.Configuration                    -- ^ e.g. from 'Aws.baseConfiguration'
        -> S3.S3Configuration Aws.NormalQuery   -- ^ e.g. 'Aws.defServiceConfig'
        -> Bucket -> Object
        -> Producer BS.ByteString m ()
fromS3WithManager mgr cfg s3cfg (Bucket bucket) (Object object) = do
    req <- liftIO $ buildRequest cfg s3cfg $ S3.getObject bucket object
    Pipes.Safe.bracket (liftIO $ responseOpen req mgr) (liftIO . responseClose) $ \resp ->
        if statusIsSuccessful (responseStatus resp)
          then from $ brRead $ responseBody resp
          else throwM $ S3DownloadError (Bucket bucket) (Object object) (responseStatus resp)

from :: MonadIO m => IO ByteString -> Producer ByteString m ()
from io = go
  where
    go = do
        bs <- liftIO io
        unless (BS.null bs) $ do
            yield bs
            go


buildRequest :: (MonadIO m, Aws.Transaction r a)
             => Aws.Configuration
             -> Aws.ServiceConfiguration r Aws.NormalQuery
             -> r
             -> m Request
buildRequest cfg scfg req = do
    let cred = Aws.credentials cfg
    sigData <- liftIO $ Aws.signatureData Aws.Timestamp cred
    let signed = Aws.signQuery req scfg sigData
    liftIO $ Aws.queryToHttpRequest signed

-- | To maintain healthy streaming uploads are performed in a chunked manner.
-- This is the size of the upload chunk size. Due to S3 interface restrictions
-- this must be at least five megabytes.
type ChunkSize = Int

type ETag = T.Text
type PartN = Integer

-- | Upload content to an S3 object.
--
-- May throw a 'EmptyS3UploadError' if the producer fails to provide any content.
toS3 :: forall m a. (MonadIO m, MonadCatch m)
     => ChunkSize -> Bucket -> Object
     -> Producer BS.ByteString m a
     -> m a
toS3 chunkSize bucket object consumer = do
    cfg <- Aws.baseConfiguration
    toS3' cfg Aws.defServiceConfig chunkSize bucket object consumer

-- | Upload content to an S3 object, explicitly specifying an
-- 'Aws.Configuration', which provides credentials and logging configuration.
--
-- May throw a 'EmptyS3UploadError' if the producer fails to provide any content.
toS3' :: forall m a. (MonadIO m, MonadCatch m)
      => Aws.Configuration                    -- ^ e.g. from 'Aws.baseConfiguration'
      -> S3.S3Configuration Aws.NormalQuery   -- ^ e.g. 'Aws.defServiceConfig'
      -> ChunkSize -> Bucket -> Object
      -> Producer BS.ByteString m a
      -> m a
toS3' cfg s3cfg chunkSize bucket object consumer = do
    mgr <- liftIO $ newManager tlsManagerSettings
    toS3WithManager mgr cfg s3cfg chunkSize bucket object consumer

-- | Download an object from S3 explicitly specifying an @http-client@ 'Manager'
-- and @aws@ 'Aws.Configuration' (which provides credentials and logging
-- configuration).
--
-- This can be more efficient when submitting many small requests as it allows
-- re-use of the 'Manager' across requests. Note that the 'Manager' provided
-- must support TLS; such a manager can be created with
--
-- @
-- 'newManager' 'HTTP.Client.TLS.tlsManagerSettings'
-- @
--
-- May throw a 'EmptyS3UploadError' if the producer fails to provide any content.
toS3WithManager :: forall m a. (MonadIO m, MonadCatch m)
      => Manager
      -> Aws.Configuration                    -- ^ e.g. from 'Aws.baseConfiguration'
      -> S3.S3Configuration Aws.NormalQuery   -- ^ e.g. 'Aws.defServiceConfig'
      -> ChunkSize -> Bucket -> Object
      -> Producer BS.ByteString m a
      -> m a
toS3WithManager mgr cfg s3cfg chunkSize bucket object consumer = do
    let Bucket bucketName = bucket
        Object objectName = object
    resp1 <- liftIO $ runResourceT
             $ Aws.pureAws cfg s3cfg mgr
             $ S3.postInitiateMultipartUpload bucketName objectName
    let uploadId = S3.imurUploadId resp1
        abortUpload err
            -- Otherwise we apparently get a 'Missing root element' error
            -- when aborting.
          | Just (EmptyS3UploadError _ _) <- fromException err = throwM err
          | otherwise = do
              resp <- liftIO $ runResourceT $ Aws.aws cfg s3cfg mgr
                             $ S3.postAbortMultipartUpload bucketName objectName uploadId
              case Aws.responseResult resp of
                Left err -> throwM err
                Right _ -> throwM $ FailedUploadError bucket object err (UploadId uploadId)

    handleAll abortUpload $ do
        let uploadPart :: (PartN, BS.ByteString) -> m (PartN, ETag)
            uploadPart (partN, content) = do
                resp <- liftIO $ runResourceT
                        $ Aws.pureAws cfg s3cfg mgr
                        $ S3.uploadPart bucketName objectName
                                        partN uploadId (RequestBodyBS content)
                return (partN, S3.uprETag resp)

        (parts, res) <- PP.toListM' $ PBS.chunksOf' chunkSize consumer
                                  >-> PP.filter (not . BS.null)
                                  >-> enumFromP 1
                                  >-> PP.mapM uploadPart

        -- We handle this specifically to provide a more sensible error than
        -- "Missing root element"
        when (null parts)
            $ throwM (EmptyS3UploadError bucket object)

        _ <- liftIO $ runResourceT
            $ Aws.pureAws cfg s3cfg mgr
            $ S3.postCompleteMultipartUpload bucketName objectName uploadId parts
        return res

enumFromP :: (Monad m, Enum i) => i -> Pipe a (i, a) m r
enumFromP = go
  where
    go i = await >>= \x -> yield (i, x) >> go (succ i)
