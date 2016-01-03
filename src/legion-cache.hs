{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE DeriveGeneric #-}
{- |
  The main program entry point for legion-cache.
-}
module Main (
  main
) where

import Prelude hiding (lookup)

import Canteven.Config (canteven)
import Canteven.Log (setupLogging)
import Control.Applicative ((<$>))
import Control.Exception (evaluate)
import Control.Monad (void)
import Control.Monad.Trans.Class (lift)
import Data.Binary (encode, decode, Binary)
import Data.Text.Lazy (unpack, Text)
import GHC.Generics (Generic)
import LegionCache.Config (Config(Config, peerAddr, joinAddr, journalFile,
  storagePath, joinTarget, port), resolveAddr)
import Network.HTTP.Types (noContent204, methodNotAllowed405)
import Network.Legion (forkLegionary, Legionary(Legionary,
  handleRequest, persistence), diskPersistence, PartitionKey(K),
  PartitionState(PartitionState), LegionarySettings(LegionarySettings,
  peerBindAddr, joinBindAddr, journal), StartupMode(JoinCluster,
  NewCluster))
import Network.Wai (requestMethod)
import Web.Scotty.Trans (ScottyT, scottyT, matchAny, request, body,
  status, header, setHeader, raw, param)
import qualified Data.ByteString.Lazy as L (ByteString)


main :: IO ()
main = do
    Config {port, peerAddr, joinAddr, joinTarget, journalFile, storagePath} <- canteven
    peerBindAddr <- resolveAddr peerAddr
    joinBindAddr <- resolveAddr joinAddr
    let settings = LegionarySettings {
        peerBindAddr,
        joinBindAddr,
        journal = journalFile
      }
    setupLogging
    let persist = diskPersistence storagePath
    mode <- case joinTarget of
      Nothing -> return NewCluster
      Just addy -> JoinCluster <$> resolveAddr addy
    handle <- forkLegionary (legionary persist) settings mode
    scottyT port id (website handle)
  where
    legionary persist = Legionary {
        handleRequest,
        persistence = persist
      }
    handleRequest _ Get Nothing = (NotFound, Nothing)
    handleRequest _ Get state@(Just (PartitionState bin)) =
      let (ct, c) = decode bin in (Val ct c, state)
    handleRequest _ (Set ct c) _ = (Ok, Just (PartitionState (encode (ct, c))))
    handleRequest _ Delete _ = (Ok, Nothing)


website :: (PartitionKey -> Request -> IO Response) -> ScottyT Text IO ()
website handle = matchAny "/:key" $ do
  key <- lift . evaluate . encodeKey =<< param "key"
  method <- requestMethod <$> request
  case method of
    "PUT" -> do
      content <- body
      contentType <- header "content-type"
      (void . lift) $ handle key (Set contentType content)
      status noContent204
    "GET" -> do
      val <- lift $ handle key Get
      case val of
        Ok -> error "this can't happen"
        NotFound -> status noContent204
        Val (Just contentType) content -> do
          setHeader "content-type" contentType
          raw content
        Val Nothing content ->
          raw content
    "DELETE" -> do
      void . lift $ handle key Delete
      status noContent204
    _ -> do
      setHeader "Allow" "GET, PUT, DELETE"
      status methodNotAllowed405


{- |
  Encode a bytestring into a Word256, by hashing it.
-}
encodeKey :: Text -> PartitionKey
encodeKey = K . read . unpack

data Request = Get | Set ContentType Content | Delete deriving (Generic)
instance Binary Request

data Response = Ok | NotFound | Val ContentType Content deriving (Generic)
instance Binary Response


type ContentType = Maybe Text
type Content = L.ByteString


