{-# LANGUAGE DeriveGeneric #-}
{- |
  This module contains the configuration data structures.
-}
module LegionCache.Config (
  Config(..),
  resolveAddr,
  AddressDescription
) where

import Canteven.Log.MonadLog (LoggingConfig)
import Data.Aeson (FromJSON)
import Data.List.Split (splitOn)
import GHC.Generics (Generic)
import Network.Socket (SockAddr, addrAddress, getAddrInfo)
import Network.Wai.Handler.Warp (Port)


data Config = Config {
    port :: Int,
    peerAddr :: AddressDescription,
    joinAddr :: AddressDescription,
    joinTarget :: Maybe AddressDescription,
    adminPort :: Port,
    adminHost :: String,
    logging :: LoggingConfig
  } deriving (Generic)

instance FromJSON Config


{- |
  An address description is really just an synonym for a formatted string.

  The only currently supported address address family is: @ipv4@

  Examples: @"ipv4:0.0.0.0:8080"@, @"ipv4:www.google.com:80"@,
-}
type AddressDescription = String


{- |
  Resolve an address description into an actual socket addr.
-}
resolveAddr :: AddressDescription -> IO SockAddr
resolveAddr desc =
  case splitOn ":" desc of
    ["ipv4", name, port_] ->
      addrAddress . head <$> getAddrInfo Nothing (Just name) (Just port_)
    _ -> error ("Invalid address description: " ++ show desc)


