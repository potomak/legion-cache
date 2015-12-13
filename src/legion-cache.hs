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
import Canteven.Snap (getMethod, noContent, notFound, methodNotAllowed)
import Control.Applicative ((<$>))
import Control.Monad (void)
import Control.Monad.IO.Class (liftIO)
import Data.Binary (encode, decode, Binary)
import Data.ByteString (ByteString)
import Data.Text (unpack)
import Data.Text.Encoding (decodeUtf8)
import GHC.Generics (Generic)
import LegionCache.Config (Config(Config, peerAddr, joinAddr, journalFile,
  storagePath, joinTarget), resolveAddr)
import Network.Legion (forkLegionary, Legionary(Legionary,
  handleRequest, persistence), diskPersistence, PartitionKey(K),
  PartitionState(PartitionState), LegionarySettings(LegionarySettings,
  peerBindAddr, joinBindAddr, journal), StartupMode(JoinCluster,
  NewCluster))
import Snap (readRequestBody, Method(GET, PUT, DELETE), getsRequest,
  rqPathInfo, getHeader, modifyResponse, setHeader, writeLBS)
import Web.Moonshine (Moonshine, runMoonshine, liftSnap)
import qualified Data.ByteString.Lazy as L (ByteString)


main :: IO ()
main = do
    Config {peerAddr, joinAddr, joinTarget, journalFile, storagePath} <- canteven
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
    runMoonshine (website handle)
  where
    legionary persist = Legionary {
        handleRequest,
        persistence = persist
      }
    handleRequest _ request state =
      case (request, state) of
        (Get, Nothing) -> (NotFound, state)
        (Get, Just (PartitionState bin)) ->
          let (ct, c) = decode bin in (Val ct c, state)
        (Set ct c, _) -> (Ok, Just (PartitionState (encode (ct, c))))
        (Delete, _) -> (Ok, Nothing)


website :: (PartitionKey -> Request -> IO Response) -> Moonshine ()
website handle = liftSnap $ do
  key <- encodeKey <$> getsRequest rqPathInfo
  method <- getMethod
  case method of
    PUT -> do
      content <- readRequestBody maxBound
      contentType <- getsRequest (getHeader "content-type")
      (void . liftIO) $ handle key (Set contentType content)
      noContent
    GET -> do
      val <- liftIO $ handle key Get
      case val of
        Ok -> error "this can't happen"
        NotFound -> notFound
        Val (Just contentType) content -> do
          modifyResponse (setHeader "content-type" contentType)
          writeLBS content
        Val Nothing content ->
          writeLBS content
    DELETE -> do
      (void . liftIO) $ handle key Delete
      noContent
    _ -> methodNotAllowed [GET, PUT, DELETE]


{- |
  Encode a bytestring into a Word256, by hashing it.
-}
encodeKey :: ByteString -> PartitionKey
encodeKey = K . read . unpack . decodeUtf8

data Request = Get | Set ContentType Content | Delete deriving (Generic)
instance Binary Request

data Response = Ok | NotFound | Val ContentType Content deriving (Generic)
instance Binary Response


type ContentType = Maybe ByteString
type Content = L.ByteString


