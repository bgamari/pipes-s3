{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Pipes.Aws.S3
   ( Bucket(..)
   , Object(..)
   , fromS3
   , toS3
   , responseBody
   ) where

import Control.Monad (unless)
import Data.String (IsString)

import qualified Data.ByteString as BS
import           Data.ByteString (ByteString)
import qualified Data.Text as T

import Pipes
import Pipes.Safe
import Pipes.Conduit
import qualified Pipes.Prelude as PP
import qualified Pipes.ByteString as PBS
import Control.Monad.Trans.Resource
import Control.Monad.IO.Class
import Network.HTTP.Client
import Network.HTTP.Client.TLS
import qualified Aws
import qualified Aws.Core as Aws
import qualified Aws.S3 as S3

newtype Bucket = Bucket T.Text
               deriving (Eq, Ord, Show, Read, IsString)
newtype Object = Object T.Text
               deriving (Eq, Ord, Show, Read, IsString)

fromS3 :: MonadSafe m
       => Bucket -> Object -> (Response (Producer BS.ByteString m ()) -> m a) -> m a
fromS3 (Bucket bucket) (Object object) handler = do
    cfg <- liftIO Aws.baseConfiguration
    let s3cfg = Aws.defServiceConfig :: S3.S3Configuration Aws.NormalQuery
    mgr <- liftIO $ newManager tlsManagerSettings
    req <- liftIO $ buildRequest cfg s3cfg $ S3.getObject bucket object
    withHTTP req mgr handler

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

type ChunkSize = Int
type ETag = T.Text
type PartN = Integer

toS3 :: forall m a. MonadIO m
     => ChunkSize -> Bucket -> Object
     -> Producer BS.ByteString m a
     -> m a
toS3 chunkSize (Bucket bucket) (Object object) consumer = do
    cfg <- Aws.baseConfiguration
    let s3cfg = Aws.defServiceConfig :: S3.S3Configuration Aws.NormalQuery
    mgr <- liftIO $ newManager tlsManagerSettings

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

    (parts, res) <- PP.toListM' $ consumer
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
