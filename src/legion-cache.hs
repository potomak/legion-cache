{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE TemplateHaskell #-}
{- |
  The main program entry point for legion-cache.
-}
module Main (
  main
) where

import Prelude hiding (lookup)

import Canteven.Config (canteven)
import Canteven.Log.MonadLog (getCantevenOutput, LoggerTImpl)
import Control.Concurrent.STM (newTVar, readTVar, writeTVar, atomically)
import Control.Exception (evaluate)
import Control.Monad (void)
import Control.Monad.IO.Class (liftIO)
import Control.Monad.Logger (runLoggingT, logDebug)
import Control.Monad.Trans.Class (lift)
import Data.Binary (Binary)
import Data.ByteString.Lazy (ByteString)
import Data.Conduit (($$), awaitForever)
import Data.Default.Class (Default, def)
import Data.List.NonEmpty (NonEmpty((:|)), (<|))
import Data.Map (Map)
import Data.String (fromString)
import Data.Text (pack)
import Data.Text.Lazy (unpack, Text)
import Data.Time.Clock (getCurrentTime)
import Data.Time.Clock.POSIX (utcTimeToPOSIXSeconds)
import GHC.Generics (Generic)
import LegionCache.Config (Config(Config, peerAddr, joinAddr, storagePath,
  joinTarget, port, adminPort, adminHost), resolveAddr)
import Network.HTTP.Types (noContent204)
import Network.Legion (forkLegionary, Legionary(Legionary,
  handleRequest, persistence), diskPersistence, PartitionKey(K),
  LegionarySettings(LegionarySettings, peerBindAddr, joinBindAddr),
  StartupMode(JoinCluster, NewCluster), ApplyDelta(apply),
  LegionConstraints, Persistence(Persistence, getState, saveState, list),
  PartitionPowerState, projected)
import Web.Scotty.Resource.Trans (resource, put, get, delete)
import Web.Scotty.Trans (ScottyT, scottyT, body, status, header,
  setHeader, raw, param)
import qualified Data.List.NonEmpty as List
import qualified Data.Map as Map
import qualified Network.Legion as L


main :: IO ()
main = do
    Config {
        port,
        peerAddr,
        joinAddr,
        joinTarget,
        storagePath,
        adminPort,
        adminHost
      } <- canteven
    peerBindAddr <- resolveAddr peerAddr
    joinBindAddr <- resolveAddr joinAddr
    let settings = LegionarySettings {
        peerBindAddr,
        joinBindAddr,
        L.adminPort = adminPort,
        L.adminHost = fromString adminHost
      }
    logging <- getCantevenOutput
    IndexedByTime {persist, oldest, newest}
      <- indexed logging (diskPersistence storagePath)
    mode <- case joinTarget of
      Nothing -> return NewCluster
      Just addy -> JoinCluster <$> resolveAddr addy
    handle <- (`runLoggingT` logging)
      $ forkLegionary (legionary persist) settings mode
    scottyT port id (website handle oldest newest)
  where
    legionary persist = Legionary {
        handleRequest,
        persistence = persist
      }
    handleRequest :: PartitionKey -> Request -> State -> Response
    handleRequest _ Get NonExistent = NotFound
    handleRequest _ Get Existent {contentType, content} =
      Val contentType content
    handleRequest _ Put {} _ = Ok
    handleRequest _ Delete _ = Ok


website
  :: (PartitionKey -> Request -> IO Response)
  -> IO (Maybe PartitionKey)
  -> IO (Maybe PartitionKey)
  -> ScottyT Text IO ()
website handle oldest newest = do
    resource "/cache/:key" $ do
      put $ do
        key <- getKey
        content <- body
        contentType <- header "content-type"
        now <- toRational . utcTimeToPOSIXSeconds <$> liftIO getCurrentTime
        (void . lift) $ handle key (Put contentType content now)
        status noContent204
      get $ do
        key <- getKey
        val <- lift $ handle key Get
        doGet val
      delete $ do
        key <- getKey
        void . lift $ handle key Delete
        status noContent204
    resource "/oldest" $
      get $
        lift oldest >>= \case
          Nothing -> status noContent204
          Just key -> doGet =<< lift (handle key Get)
    resource "/newest" $
      get $
        lift newest >>= \case
          Nothing -> status noContent204
          Just key -> doGet =<< lift (handle key Get)
        
  where
    getKey = lift . evaluate . encodeKey =<< param "key"
    doGet val =
      case val of
        Ok -> error "this can't happen"
        NotFound -> status noContent204
        Val (Just contentType) content -> do
          setHeader "content-type" contentType
          raw content
        Val Nothing content ->
          raw content


{- |
  Encode a bytestring into a Word256, the stupid way.
-}
encodeKey :: Text -> PartitionKey
encodeKey = K . read . unpack


data Request
  = Get
  | Put (Maybe ContentType) Content Rational
  | Delete deriving (Generic, Show, Eq)
instance Binary Request
instance ApplyDelta Request State where
  apply Get s = s
  apply (Put ct c t) _ = Existent ct c t
  apply Delete _ = NonExistent


data Response
  = Ok
  | NotFound
  | Val (Maybe ContentType) Content
  deriving (Generic, Show)
instance Binary Response


data State
  = NonExistent
  | Existent {
      contentType :: Maybe ContentType,
          content :: Content,
             time :: Rational
    }
  deriving (Generic, Show)
instance Binary State
instance Default State where
  def = NonExistent


type ContentType = Text


type Content = ByteString


instance LegionConstraints Request Response State where


{- |
  A `Persistence` layer that maintains an index by time.
-}
data IndexedByTime = IndexedByTime {
    persist :: Persistence Request State,
     oldest :: IO (Maybe PartitionKey),
     newest :: IO (Maybe PartitionKey)
  }


indexed :: LoggerTImpl -> Persistence Request State -> IO IndexedByTime
indexed logging p = do
    byTimeT <- atomically $
      newTVar (Map.empty :: Map Rational (NonEmpty PartitionKey))
    let
      search
        :: (
            {-
              This honker is the type of a "view" into the index,
              a. la. 'Map.minView'
            -}
            Map Rational (NonEmpty PartitionKey)
            -> Maybe (NonEmpty PartitionKey, Map Rational (NonEmpty PartitionKey))
          )
        -> IO (Maybe PartitionKey)
      search view = do
        debugM . ("Index: " ++) . show =<< atomically (readTVar byTimeT)
        atomically $
          view <$> readTVar byTimeT >>= \case
            Nothing -> return Nothing
            Just (key :| _, _) -> return (Just key)

      oldest :: IO (Maybe PartitionKey)
      oldest = search Map.minView

      newest :: IO (Maybe PartitionKey)
      newest = search Map.maxView

      unindex key = do
        byTime <- readTVar byTimeT
        writeTVar byTimeT $ Map.fromAscList [
            (t, k)
            | (t, keys) <- Map.toAscList byTime
            , k <-
              case List.filter (/= key) keys of
                [] -> []
                a:more -> [a:|more]
          ]

      index :: PartitionKey -> PartitionPowerState Request State -> IO ()
      index key ps = atomically $ do
        unindex key
        byTime <- readTVar byTimeT
        writeTVar byTimeT (
            case projected ps of
              NonExistent -> byTime
              Existent {time} ->
                let
                  update Nothing = Just (key:|[])
                  update (Just keys) = Just (key <| keys)
                in Map.alter update time byTime
          )

    list p $$ awaitForever (lift . uncurry index)
      
    return IndexedByTime {
        persist = Persistence {
            getState = getState p,
            saveState = \ key state -> do
              debugM $ "Saving: " ++ show (key, state)
              case state of
                Nothing -> atomically (unindex key)
                Just ps -> index key ps
              saveState p key state,
            list = list p
          },
        oldest,
        newest
      }
  where
    debugM = (`runLoggingT` logging) . $(logDebug) . pack 


