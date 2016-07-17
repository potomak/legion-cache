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
import LegionCache.Config (Config(Config, peerAddr, joinAddr, joinTarget,
  port, adminPort, adminHost), resolveAddr)
import Network.HTTP.Types (noContent204)
import Network.Legion (forkLegionary, Legionary(Legionary,
  handleRequest, persistence), newMemoryPersistence, PartitionKey(K),
  LegionarySettings(LegionarySettings, peerBindAddr, joinBindAddr),
  StartupMode(JoinCluster, NewCluster), ApplyDelta(apply),
  LegionConstraints, Persistence(Persistence, getState, saveState, list),
  PartitionPowerState, projected)
import Web.Scotty (ScottyM, scotty, body, status, header, setHeader,
  raw, param)
import Web.Scotty.Resource.Trans (resource, put, get, delete)
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
      <- indexed logging =<< newMemoryPersistence
    mode <- case joinTarget of
      Nothing -> return NewCluster
      Just addy -> JoinCluster <$> resolveAddr addy

    {-
      Fork the Legion runtime process in the background, returning a "handler"
      function that we can use to send 'Request's to the Legion runtime for
      execution.

      handle :: PartitionKey -> Request -> IO Response
    -}
    handle <- (`runLoggingT` logging)
      $ forkLegionary (legionary persist) settings mode

    {- |
      Run a scotty application using the warp server. The scotty
      application transforms HTTP requests into our Legion application's
      'Request' type, and passes those requests to the Legion runtime.
    -}
    scotty port (website handle oldest newest)
  where
    {-
      Construct the Legionary value that defines the application to be
      run by the Legion framework.

      See: https://hackage.haskell.org/package/legion-0.1.0.0/docs/Network-Legion.html#t:Legionary
    -}
    legionary persist = Legionary {
        handleRequest,
        persistence = persist
      }

    {-
      This is the application request handler which is run by the Legion
      framework. This, combined with a persistence layer, make up the entirety
      of the Legion application.

      Legion-Cache is a key/value store, so this particular request
      handler turns out to be very simple. There are 3 possible
      operations: Get, Put, and Delete.

      The 'PartitionKey' type is defined by the Legion framework, and indicates
      to which partition the request applies.

      The 'Request', 'State', and 'Response' types, on the other hand, are
      defined by us, and make up an important part of the definition of the
      application we are asking Legion to run. Those types make up the types
      upon which our application logic operates, and this function implements
      the logic itself.
    -}
    handleRequest :: PartitionKey -> Request -> State -> Response
    handleRequest _ Get NonExistent = NotFound
    handleRequest _ Get Existent {contentType, content} =
      Val contentType content
    handleRequest _ Put {} _ = Ok
    handleRequest _ Delete _ = Ok


{- |
  This is the type of request that our Legion application will handle.
  
  Our application implements a key/value store, so:

  'Get' means retrieve the current value of the partition.
  'Put' means set the value of the partition to the provided value, which
        includes the content-type, some content, and a time stamp indicating
        age.
  'Delete' means wipe out the current value, if there is one.

  We have to implement these semantics ourselves, in the application request
  handler, which is named 'handleRequest', just above.
-}
data Request
  = Get
  | Put (Maybe ContentType) Content Time
  | Delete deriving (Generic, Show, Eq)
{-
  Requests must be an instance of 'Binary' because they will probably have to
  be transmitted across the network.
-}
instance Binary Request
{-
  Requests must be an instance of 'ApplyDelta'. This lets the Legion framework
  know how each request might mutate the partition state.
-}
instance ApplyDelta Request State where
  {- 'Get' does not mutate the partition state at all.  -}
  apply Get s = s 

  {-
    'Put' mutates the partition state by setting it to exactly the
    ('Existent') value provided, overriding any previous value.
  -}
  apply (Put ct c t) _ = Existent ct c t

  {-
    'Delete' mutates the partition state by setting it to 'NonExistent',
    overriding any previous value.
  -}
  apply Delete _ = NonExistent


{- |
  This is the type of state needed by our Legion application. Each
  partition key is associated with a value of this type. The Legion-Cache
  application is modeled so that each partition corresponds with a single
  "value" in our key/value store (and each "key" with the corresponding
  'PartitionKey').

  We are implementing a key/value store where one partition represents one
  entry in the store, so we define each partition to be either:

  'NonExistent', meaning our key/value store does not contain an entry for the
    associated key; or

  'Existent', meaning the value associated with the key exists and
    possesses the attributes associated with this data constructor
    (i.e. contentType, content, and time).

-}
data State
  = NonExistent
  | Existent {
      contentType :: Maybe ContentType,
          content :: Content,
        timeStamp :: Time
    }
  deriving (Generic, Show)
instance Binary State
instance Default State where
  def = NonExistent


{- |
  These are the types of responses that we have defined for our application.
  The values are:

  'Ok', used as the response to 'Put' and 'Delete' requests.

  'NotFound', used as the response to a 'Get' request, when the partition value
    is 'NonExistent'

  'Val', used as the response to a 'Get' request when the partition value is
    'Existent'

  Note! At this time it is not possible to bind a particular type of
  response to a particular type of request at the type-system level. So,
  there is nothing preventing our handler ('handleRequest', defined above)
  from returning 'Ok' in response to a 'Get' request (which would make
  no sense for a key/value store). It *is* possible to achieve this
  kind of binding using the Haskell type system, though, and we will
  strongly consider adding this capability to the Legion framework in
  the future; but for now Legion is still in the experimental phase,
  so we want to keep it as simple as possible until we are sure we have
  a solid underlying core.
-}
data Response
  = Ok
  | NotFound
  | Val (Maybe ContentType) Content
  deriving (Generic, Show)
instance Binary Response


website
  :: (PartitionKey -> Request -> IO Response)
  -> IO (Maybe PartitionKey)
  -> IO (Maybe PartitionKey)
  -> ScottyM ()
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


type ContentType = Text


type Content = ByteString


instance LegionConstraints Request Response State where


{- |
  A `Persistence` layer that maintains an index by time.

  The index implemented here is completely outside of the Legion
  framework. It is an example of how a program using the Legion framework
  might keep track of data for its own purposes. Right now we are
  using this as the basis for an experimental map-reduce prototype
  (e.g. finding the oldest or newest object in the entire cluster),
  but this is just an experiment. We will almost certainly build real
  map-reduce capability into Legion itself at some point in the future.

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
              Existent {timeStamp} ->
                let
                  update Nothing = Just (key:|[])
                  update (Just keys) = Just (key <| keys)
                in Map.alter update timeStamp byTime
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


{- |
  Time is represented as a 'Rational' because UTCTime does not have a
  'Binary' instance.
-}
type Time = Rational


