{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE LambdaCase #-}
{- |
  This module contains the configuration data structures.
-}
module LegionCache.Config (
  Config(..),
  resolveAddr,
  AddressDescription,
  parseArgs,
) where

import Canteven.Log.MonadLog (LoggingConfig)
import Data.Aeson (FromJSON)
import Data.Default.Class (Default(def))
import Data.List (intercalate)
import Data.List.Split (splitOn)
import Data.Yaml (decodeFileEither)
import GHC.Generics (Generic)
import Network.Legion (StartupMode(JoinCluster, NewCluster))
import Network.Socket (SockAddr, addrAddress, getAddrInfo)
import Network.Wai.Handler.Warp (Port)
import System.Console.GetOpt (OptDescr(Option), usageInfo, getOpt,
  ArgOrder(Permute), ArgDescr(ReqArg))
import System.Environment (getArgs, getProgName)


data Config = Config {
    port :: Int,
    peerAddr :: AddressDescription,
    joinAddr :: AddressDescription,
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


{- |
  Parse the command line arguments.
-}
parseArgs :: IO (Config, StartupMode)
parseArgs = do
    prog <- getProgName
    getOpt Permute options <$> getArgs >>= \case
      (opts, [], []) ->
        case foldr (.) id opts def of
          Opt Nothing _ ->
            {- No config file.  -}
            fail $ "Missing config file: \n\n" ++  usageInfo prog options
          Opt (Just configFile) maybeJoinTarget ->
            decodeFileEither configFile >>= \case
              Left errorMsg -> fail $
                "Couldn't decode YAML config from file "
                ++ configFile ++ ": " ++ show errorMsg
              Right config ->
                case maybeJoinTarget of
                  Nothing -> return (config, NewCluster)
                  Just jtDesc -> do
                    jt <- resolveAddr jtDesc
                    return (config, JoinCluster jt)
      (_, [], errors) ->
        fail (intercalate "\n" (errors ++ [usageInfo prog options]))
      (_, unknown, _) ->
        fail
          $ "Unknown options: " ++ intercalate ", " unknown
          ++ "\n" ++ usageInfo prog options
  where
    options = [
        Option
          ['c']
          ["config"]
          (ReqArg
              (\name opt -> opt {oConfigFile = Just name})
              "<file>"
            )
          "Specifies the config file.",
        Option
          ['j']
          ["joinTarget"]
          (ReqArg
              (\addr opt -> opt {oJoinTarget = Just addr})
              "ipv4:<host>:<port>"
            )
          "The address of a node in the cluster we want to join."
      ]

data Opt = Opt {
    oConfigFile :: Maybe FilePath,
    oJoinTarget :: Maybe AddressDescription
  }
instance Default Opt where
  def = Opt Nothing Nothing


