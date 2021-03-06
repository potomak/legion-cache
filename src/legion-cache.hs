{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedStrings #-}
{- |
  The main program entry point for legion-cache.
-}
module Main (
  main
) where

import Prelude hiding (lookup)

import Canteven.Log.MonadLog (getCantevenOutput)
import Control.Exception (evaluate)
import Control.Monad (void)
import Control.Monad.IO.Class (liftIO)
import Control.Monad.Logger (runLoggingT)
import Control.Monad.Trans.Class (lift)
import Data.Binary (Binary)
import Data.ByteString.Lazy (ByteString)
import Data.Conduit (($$))
import Data.Default.Class (Default, def)
import Data.Set (Set)
import Data.String (fromString)
import Data.Text (pack)
import Data.Text.Encoding (encodeUtf8)
import Data.Text.Lazy (unpack, Text)
import Data.Time (getCurrentTime, formatTime, defaultTimeLocale)
import Data.Time.Clock.POSIX (utcTimeToPOSIXSeconds, posixSecondsToUTCTime)
import GHC.Generics (Generic)
import LegionCache.Config (Config(Config, peerAddr, joinAddr, port,
  adminPort, adminHost), resolveAddr, parseArgs, ekgPort)
import Network.HTTP.Types (noContent204)
import Network.Legion (forkLegionary, Legionary(Legionary, index,
  handleRequest, persistence), newMemoryPersistence, PartitionKey(K),
  LegionarySettings(LegionarySettings, peerBindAddr, joinBindAddr),
  ApplyDelta(apply), Runtime, Tag(Tag), search, SearchTag(SearchTag),
  makeRequest, IndexRecord(IndexRecord))
import Web.Scotty (ScottyM, scotty, body, status, header, setHeader,
  raw, param)
import Web.Scotty.Resource.Trans (resource, put, get, delete)
import qualified Data.ByteString.Lazy as BL
import qualified Data.Conduit.List as CL
import qualified Data.Set as Set
import qualified LegionCache.Config as C
import qualified Network.Legion as L
import qualified System.Remote.Monitoring as Ekg


main :: IO ()
main = do
    (
        Config {
            port,
            peerAddr,
            joinAddr,
            adminPort,
            adminHost,
            ekgPort,
            C.logging = loggingConfig
          },
        startupMode
      ) <- parseArgs

    {- start EKG -}
    void $ Ekg.forkServer "localhost" ekgPort

    peerBindAddr <- resolveAddr peerAddr
    joinBindAddr <- resolveAddr joinAddr
    let settings = LegionarySettings {
        peerBindAddr,
        joinBindAddr,
        L.adminPort = adminPort,
        L.adminHost = fromString adminHost
      }
    logging <- getCantevenOutput loggingConfig
    persist <- newMemoryPersistence

    {-
      Fork the Legion runtime process in the background, returning a
      handle that we can use to send 'Request's to the Legion runtime
      for execution.
    -}
    runtime <- (`runLoggingT` logging)
      $ forkLegionary (legionary persist) settings startupMode

    {- |
      Run a scotty application using the warp server. The scotty
      application transforms HTTP requests into our Legion application's
      'Request' type, and passes those requests to the Legion runtime.
    -}
    scotty port (website runtime)
  where
    {-
      Construct the Legionary value that defines the application to be
      run by the Legion framework.

      See: https://hackage.haskell.org/package/legion-0.1.0.0/docs/Network-Legion.html#t:Legionary
    -}
    legionary persist = Legionary {
        handleRequest,
        persistence = persist,
        index
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
      This informs the legion framework how we want to to index our partitions.
      In this case, we base it on the time of the cache entry. Legion index
      tags are sorted lexicographically, so we need to use a time format that
      sorts correctly. It turns out, ISO-8601 Zulu time works just fine for
      this.
    -}
    index :: State -> Set Tag
    index NonExistent = Set.empty
    index Existent {timeStamp} =
      let
        utcTime = posixSecondsToUTCTime (fromRational timeStamp)
        tag = "time=" ++ formatTime defaultTimeLocale "%FT%XZ" utcTime
      in (Set.singleton . Tag . encodeUtf8 . pack) tag


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
  | Delete
  deriving (Generic, Eq)
{-
  Requests must be an instance of 'Binary' because they will probably have to
  be transmitted across the network.
-}
instance Binary Request
instance Show Request where
  show Get = "Get"
  show (Put ct c t) =
    "(Put " ++ show ct ++ " <" ++ show (BL.length c) ++ " bytes> "
    ++ show t ++ ")"
  show Delete = "Delete"
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
  deriving (Generic)
instance Binary State
instance Default State where
  def = NonExistent
instance Show State where
  show NonExistent = "NonExistent"
  show (Existent ct c t) = 
    "Existent {contentType = " ++ show ct ++ ", content = <"
    ++ show (BL.length c) ++ " bytes>, timeStamp = " ++ show t ++ "}"


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
  :: Runtime Request Response
  -> ScottyM ()
website runtime = do
    resource "/cache/:key" $ do
      put $ do
        key <- getKey
        content <- body
        contentType <- header "content-type"
        now <- toRational . utcTimeToPOSIXSeconds <$> liftIO getCurrentTime
        void $ makeRequest runtime key (Put contentType content now)
        status noContent204
      get $ do
        key <- getKey
        val <- makeRequest runtime key Get
        doGet val
      delete $ do
        key <- getKey
        void $ makeRequest runtime key Delete
        status noContent204
    resource "/oldest" $
      get $
        (search runtime (SearchTag "time=" Nothing) $$ CL.take 1) >>= \case
          [] -> status noContent204
          IndexRecord _ key:_ -> doGet =<< makeRequest runtime key Get

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


{- |
  Time is represented as a 'Rational' because UTCTime does not have a
  'Binary' instance.
-}
type Time = Rational


