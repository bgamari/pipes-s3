{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE GADTs #-}

-- | A simple streaming interface to the AWS S3 storage service.
module Pipes.Aws.S3
   ( -- * Basic types
     Bucket(..)
   , Object(..)
     -- * Downloading
   , module Pipes.Aws.S3.Download
     -- ** With retries
   , module Pipes.Aws.S3.Download.Retry

     -- * Uploading
   , module Pipes.Aws.S3.Upload
   ) where

import Pipes.Aws.S3.Download
import Pipes.Aws.S3.Download.Retry
import Pipes.Aws.S3.Upload
import Pipes.Aws.S3.Types
