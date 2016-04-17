module Pipes.Conduit where

import Control.Monad.Trans.Class
import qualified Data.Conduit as C
import qualified Data.Conduit.Internal as CI
import qualified Pipes as P

fromSource :: Monad m => C.Source m a -> P.Producer a m ()
fromSource = go . CI.newResumableSource
  where
    go src = do
        (src', mx) <- lift $ src C.$$++ C.await
        case mx of
            Nothing -> lift $ CI.closeResumableSource src'
            Just x  -> P.yield x >> go src'

toSource :: Monad m => P.Producer a m () -> C.Source m a
toSource = go
  where
    go prod = do
        mx <- lift $ P.next prod
        case mx of
            Left ()           -> return ()
            Right (x, prod')  -> C.yield x >> go prod'
