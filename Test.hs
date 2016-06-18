{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

import Pipes
import Pipes.Safe
import qualified Pipes.Aws.S3 as S3
import Pipes.ByteString as PBS
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BSL
import Test.QuickCheck
import Test.QuickCheck.Monadic

main :: IO ()
main = do
    quickCheck $ propRoundTrip bucket object
    quickCheck $ propFailure bucket object
    quickCheck $ propEmptyFails bucket object
  where
    bucket = S3.Bucket "bgamari-test"
    object = S3.Object "test"

newtype ChunkSize = ChunkSize Int
                  deriving (Show, Enum)

megabyte = 1024*1024

instance Bounded ChunkSize where
    minBound = ChunkSize $ 5*megabyte
    maxBound = ChunkSize $ 2*1024*megabyte

instance Arbitrary ChunkSize where
    arbitrary = arbitraryBoundedEnum

data Outcome = Succeeds | Fails
             deriving (Enum, Bounded)

instance Arbitrary Outcome where
    arbitrary = arbitraryBoundedEnum

data FailureException = FailureException
                      deriving (Show)

instance Exception FailureException

instance Arbitrary BS.ByteString where
    arbitrary = BS.pack . getNonEmpty <$> arbitrary

instance Arbitrary BSL.ByteString where
    arbitrary = BSL.fromChunks . getNonEmpty <$> arbitrary

propEmptyFails :: S3.Bucket -> S3.Object -> ChunkSize -> Property
propEmptyFails bucket object (ChunkSize chunkSize) = monadicIO $ do
    run $ handle checkException $ do
        S3.toS3 chunkSize bucket object (each $ replicate 5 BS.empty)
        fail "empty uploads should fail"
  where
    checkException (S3.EmptyS3UploadError _ _) = return ()

propRoundTrip :: S3.Bucket -> S3.Object -> ChunkSize -> BSL.ByteString -> Property
propRoundTrip bucket object (ChunkSize chunkSize) content = monadicIO $ do
    run $ S3.toS3 chunkSize bucket object (each $ BSL.toChunks content)
    content' <- run $ runSafeT $ PBS.toLazyM $ S3.fromS3 bucket object S3.responseBody
    return $ content == content'

propFailure :: S3.Bucket -> S3.Object -> ChunkSize -> BSL.ByteString -> Property
propFailure bucket object (ChunkSize chunkSize) content = monadicIO $ do
    run $ handle handleFailure $ do
        S3.toS3 chunkSize bucket object (each (BSL.toChunks content) >> throwM FailureException)
        fail "unexpectedly succeeded"
  where
    handleFailure (S3.FailedUploadError {S3.failedUploadException = exc})
      | Just FailureException <- fromException exc = return ()
      | otherwise = fail $ "failed with exception: "++show exc
