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
import Network.Legion (forkLegionary, Legionary(Legionary,
  handleRequest, persistence), newMemoryPersistence, PartitionKey(K),
  PartitionState(PartitionState), LegionarySettings(LegionarySettings,
  peerBindAddr, discovery, stateFile))
import Snap (readRequestBody, Method(GET, PUT, DELETE), getsRequest,
  rqPathInfo, getHeader, modifyResponse, setHeader, writeLBS)
import Web.Moonshine (Moonshine, runMoonshine, liftSnap)
import qualified Data.ByteString.Lazy as L (ByteString)


main :: IO ()
main = do
    setupLogging
    memstore <- newMemoryPersistence
    handle <- forkLegionary (legionary memstore) settings
    runMoonshine (website handle)
  where
    settings = LegionarySettings {
        peerBindAddr = "ipv4:localhost:8002",
        discovery = error "legion-cache: undefined discovery",
        stateFile = "legion-state"
      }
    legionary memstore = Legionary {
        handleRequest,
        persistence = memstore
      }
    handleRequest _ request state =
      case (request, state) of
        (Get, Nothing) -> (NotFound, state)
        (Get, Just (PartitionState bin)) -> let (ct, c) = decode bin in (Val ct c, state)
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


