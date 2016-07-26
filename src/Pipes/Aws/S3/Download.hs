module Pipes.Aws.S3.Download
    ( -- | These may fail with either an 'S3DownloadError' or a 'Aws.S3Error'.
      fromS3
    , fromS3'
    , fromS3WithManager
    , S3DownloadError(..)
    ) where

import Control.Monad (unless)
import Control.Exception (Exception)

import qualified Data.ByteString as BS
import           Data.ByteString (ByteString)

import Pipes
import Pipes.Safe
import Network.HTTP.Types
import Network.HTTP.Client
import Network.HTTP.Client.TLS
import qualified Aws
import qualified Aws.Core as Aws
import qualified Aws.S3 as S3

import Pipes.Aws.S3.Types

-- | Thrown when an unknown status code is returned from an S3 download request.
data S3DownloadError = S3DownloadError Bucket Object Status
                     deriving (Show)

instance Exception S3DownloadError

-- | Download an object from S3
--
-- Note that this makes no attempt at reusing a 'Manager' and therefore may not
-- be very efficient for many small requests. See 'fromS3WithManager' for more
-- control over the 'Manager' used.
fromS3 :: MonadSafe m
       => Bucket -> Object
       -> Maybe ContentRange
       -- ^ The requested 'ContentRange'. 'Nothing' implies entire object.
       -> Producer BS.ByteString m ()
fromS3 bucket object range = do
    cfg <- liftIO Aws.baseConfiguration
    fromS3' cfg Aws.defServiceConfig bucket object range

-- | Download an object from S3 explicitly specifying an @aws@ 'Aws.Configuration',
-- which provides credentials and logging configuration.
--
-- Note that this makes no attempt at reusing a 'Manager' and therefore may not
-- be very efficient for many small requests. See 'fromS3WithManager' for more
-- control over the 'Manager' used.
fromS3' :: MonadSafe m
        => Aws.Configuration                    -- ^ e.g. from 'Aws.baseConfiguration'
        -> S3.S3Configuration Aws.NormalQuery   -- ^ e.g. 'Aws.defServiceConfig'
        -> Bucket -> Object -> Maybe ContentRange
        -> Producer BS.ByteString m ()
fromS3' cfg s3cfg bucket object range = do
    mgr <- liftIO $ newManager tlsManagerSettings
    fromS3WithManager mgr cfg s3cfg bucket object range

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
        -> Bucket -> Object -> Maybe ContentRange
        -> Producer BS.ByteString m ()
fromS3WithManager mgr cfg s3cfg (Bucket bucket) (Object object) range = do
    let getObj = (S3.getObject bucket object) { S3.goResponseContentRange = fmap (\(ContentRange a b) -> (a,b)) range }
    req <- liftIO $ buildRequest cfg s3cfg getObj
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
