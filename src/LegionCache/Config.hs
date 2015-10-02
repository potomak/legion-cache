{-# LANGUAGE DeriveGeneric #-}
{- |
  This module contains the configuration data structures.
-}
module LegionCache.Config (
  Config(..)
) where

import Data.Aeson (FromJSON)
import GHC.Generics (Generic)
import Network.Legion (AddressDescription, StartupMode)

data Config = Config {
    peerAddr :: AddressDescription,
    startupMode :: StartupMode
  } deriving (Generic)

instance FromJSON Config


