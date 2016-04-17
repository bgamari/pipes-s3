{-# LANGUAGE OverloadedStrings #-}

import qualified Pipes.Aws.S3 as S3
import qualified Pipes.GZip as GZip
import qualified Pipes.ByteString as PBS
import Pipes
import qualified Pipes.Prelude as PP


bucket = "aws-publicdatasets"
object = "common-crawl/crawl-data/CC-MAIN-2015-40/segments/1443736672328.14/warc/CC-MAIN-20151001215752-00004-ip-10-137-6-227.ec2.internal.warc.gz"

main :: IO ()
main = do
    r <- S3.fromS3 bucket object $ \resp -> do
        runEffect $ GZip.decompress (S3.responseBody resp) >-> PBS.stdout
    print r
    return ()

