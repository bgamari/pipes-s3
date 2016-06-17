{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- | A simple streaming interface to the AWS S3 storage service.
module Pipes.Aws.S3
   ( Bucket(..)
   , Object(..)
     -- * Downloading
   , fromS3
   , fromS3'
   , fromS3WithManager
     -- ** Convenient re-exports
   , responseBody
     -- * Uploading
     -- | These internally use the S3 multi-part upload interface to achieve
     -- streaming upload behavior.
   , ChunkSize
   , toS3
   , toS3'
   , toS3WithManager
   ) where

import Control.Monad (unless)
import Data.String (IsString)

import qualified Data.ByteString as BS
import           Data.ByteString (ByteString)
import qualified Data.Text as T

import Pipes
import Pipes.Safe
import qualified Pipes.Prelude as PP
import qualified Pipes.ByteString as PBS
import Control.Monad.Trans.Resource
import Control.Monad.IO.Class
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

-- | Download an object from S3
--
-- This initiates an S3 download, requiring that the caller provide a way to
-- construct a 'Producer' from the initial 'Response' to the request (allowing
-- the caller to, e.g., handle failure).
--
-- For instance to merely produced the content of the response,
--
-- @
-- 'fromS3' bucket object responseBody
-- @
--
-- Note that this makes no attempt at reusing a 'Manager' and therefore may not
-- be very efficient for many small requests. See 'fromS3WithManager' for more
-- control over the 'Manager' used.
fromS3 :: MonadSafe m
       => Bucket -> Object
       -> (Response (Producer BS.ByteString m ()) -> Producer BS.ByteString m a)
       -> Producer BS.ByteString m a
fromS3 bucket object handler = do
    cfg <- liftIO Aws.baseConfiguration
    fromS3' cfg bucket object handler

-- | Download an object from S3 explicitly specifying an @aws@ 'Aws.Configuration',
-- which provides credentials and logging configuration.
--
-- Note that this makes no attempt at reusing a 'Manager' and therefore may not
-- be very efficient for many small requests. See 'fromS3WithManager' for more
-- control over the 'Manager' used.
fromS3' :: MonadSafe m
        => Aws.Configuration -> Bucket -> Object
        -> (Response (Producer BS.ByteString m ()) -> Producer BS.ByteString m a)
        -> Producer BS.ByteString m a
fromS3' cfg bucket object handler = do
    mgr <- liftIO $ newManager tlsManagerSettings
    fromS3WithManager mgr cfg Aws.defServiceConfig bucket object handler

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
        -> Aws.Configuration
        -> S3.S3Configuration Aws.NormalQuery
        -> Bucket -> Object
        -> (Response (Producer BS.ByteString m ()) -> Producer BS.ByteString m a)
        -> Producer BS.ByteString m a
fromS3WithManager mgr cfg s3cfg (Bucket bucket) (Object object) handler = do
    req <- liftIO $ buildRequest cfg s3cfg $ S3.getObject bucket object
    Pipes.Safe.bracket (liftIO $ responseOpen req mgr) (liftIO . responseClose) $ \resp ->
        handler $ resp { responseBody = from $ brRead $ responseBody resp }

-- Stolen from pipes-http
withHTTP :: MonadSafe m
         => Request
         -> Manager
         -> (Response (Producer ByteString m ()) -> m a)
         -> m a
withHTTP req mgr k =
    Pipes.Safe.bracket (liftIO $ responseOpen req mgr) (liftIO . responseClose) k'
  where
    k' resp = do
        let p = (from . brRead . responseBody) resp
        k (resp { responseBody = p})

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
    Just cred <- Aws.loadCredentialsDefault
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
toS3 :: forall m a. MonadIO m
     => ChunkSize -> Bucket -> Object
     -> Producer BS.ByteString m a
     -> m a
toS3 chunkSize bucket object consumer = do
    cfg <- Aws.baseConfiguration
    toS3' cfg chunkSize bucket object consumer

-- | Upload content to an S3 object, explicitly specifying an
-- 'Aws.Configuration', which provides credentials and logging configuration.
toS3' :: forall m a. MonadIO m
      => Aws.Configuration -> ChunkSize -> Bucket -> Object
      -> Producer BS.ByteString m a
      -> m a
toS3' cfg chunkSize bucket object consumer = do
    mgr <- liftIO $ newManager tlsManagerSettings
    toS3WithManager mgr cfg Aws.defServiceConfig chunkSize bucket object consumer

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
toS3WithManager :: forall m a. MonadIO m
      => Manager -> Aws.Configuration -> S3.S3Configuration Aws.NormalQuery
      -> ChunkSize -> Bucket -> Object
      -> Producer BS.ByteString m a
      -> m a
toS3WithManager mgr cfg s3cfg chunkSize (Bucket bucket) (Object object) consumer = do
    resp1 <- liftIO $ runResourceT
             $ Aws.pureAws cfg s3cfg mgr
             $ S3.postInitiateMultipartUpload bucket object
    let uploadId = S3.imurUploadId resp1

    let uploadPart :: (PartN, BS.ByteString) -> m (PartN, ETag)
        uploadPart (partN, content) = do
            resp <- liftIO $ runResourceT
                    $ Aws.pureAws cfg s3cfg mgr
                    $ S3.uploadPart bucket object partN uploadId (RequestBodyBS content)
            return (partN, S3.uprETag resp)

    (parts, res) <- PP.toListM' $ PBS.chunksOf' chunkSize consumer
                              >-> enumFromP 1
                              >-> PP.mapM uploadPart

    resp2 <- liftIO $ runResourceT
             $ Aws.pureAws cfg s3cfg mgr
             $ S3.postCompleteMultipartUpload bucket object uploadId parts
    return res


enumFromP :: (Monad m, Enum i) => i -> Pipe a (i, a) m r
enumFromP = go
  where
    go i = await >>= \x -> yield (i, x) >> go (succ i)
