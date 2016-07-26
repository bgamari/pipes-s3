{-# LANGUAGE GeneralizedNewtypeDeriving #-}

-- | Basic types used throughout @pipes-s3@.
module Pipes.Aws.S3.Types
    ( Bucket(..)
    , Object(..)
    ) where

import Data.String (IsString)
import qualified Data.Text as T

-- | An AWS S3 bucket name
newtype Bucket = Bucket T.Text
               deriving (Eq, Ord, Show, Read, IsString)

-- | An AWS S3 object name
newtype Object = Object T.Text
               deriving (Eq, Ord, Show, Read, IsString)
