{-# LANGUAGE OverloadedStrings, DeriveDataTypeable, DeriveGeneric, BangPatterns #-}

module Main where

import Control.Monad
import Control.Monad.IO.Class (liftIO)
import Control.Concurrent
import Control.Exception

import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as LB
import qualified Data.ByteString.Lex.Double as B
import qualified Data.ByteString.Lex.Integral as B
import Data.Int (Int64)
import qualified Data.ProtocolBuffers as P
import qualified Data.Serialize.Put as P
import Data.TypeLevel (D1, D2, D3, D4, D5, D6)
import GHC.Generics (Generic)

import Data.Csv
import qualified Data.Csv.Incremental as I
import qualified Data.Csv.Parser as I
import Data.Either
import Data.Maybe (fromJust)
import qualified Data.Vector as V

import Network.Socket

import System.Console.CmdArgs.Implicit
import System.IO
import qualified System.Random as R
import System.Log.FastLogger
import qualified System.IO.Streams.File as S
import qualified System.IO.Streams as S

import qualified Text.Show.ByteString as SB

type Price = Double
type Volume = Int64

data TickRecord = TickRecord 
    { _date    :: B.ByteString
    , _time     :: B.ByteString
    , _price    :: Price
    , _bid      :: Price
    , _ask      :: Price
    , _volume   :: Volume
    }

instance FromRecord TickRecord where
    parseRecord v
        | V.length v == 6 = do
            date' <- v .! 0
            time' <- v .! 1
            price' <- v .! 2
            bid' <- v .! 3
            ask' <- v .! 4
            volume' <- v .! 5
            return $! TickRecord
                { _date = date',
                  _time = time',
                  _price = (fst . fromJust . B.readDouble) price',
                  _bid = (fst . fromJust . B.readDouble) bid',
                  _ask = (fst . fromJust . B.readDouble) ask',
                  _volume = (fst . fromJust . B.readDecimal) volume'
                }
        | otherwise = mzero


data TickProtobuf = TickProtobuf
   { pDate :: P.Required D1 (P.Value B.ByteString)
   , pTime :: P.Required D2 (P.Value B.ByteString)
   , pPrice :: P.Required D3 (P.Value Double)
   , pBid :: P.Required D4 (P.Value Double)
   , pAsk :: P.Required D5 (P.Value Double)
   , pVolume :: P.Required D6 (P.Value Int64)
   } deriving (Generic, Show)

instance P.Encode TickProtobuf
instance P.Decode TickProtobuf


decodeWith :: FromRecord b => DecodeOptions -> Bool -> S.InputStream B.ByteString -> IO (S.InputStream b)
decodeWith !opts skipHeader iis = S.fromGenerator (go V.empty iis start)
    where
        start = (I.decodeWith opts skipHeader `I.feedChunk`)
        go !buf is' k = do
            {-liftIO $ print $ V.length buf-}
            if V.null buf
                then do
                    s0 <- liftIO $ S.read is'
                    case s0 of
                        Just s -> do process buf is' (k s)
                        Nothing -> do process buf is' (I.feedEndOfInput $ I.decodeWith opts skipHeader)
                else do
                    {-liftIO $ print $ V.length buf-}
                    S.yield $ V.last buf
                    go (V.init buf) is' k
        process !buf is (I.Done xs) = do let buf' = buf V.++ (V.reverse . V.fromList . rights $ xs)
                                         go buf' is start
        process !buf is (I.Fail rest err) = undefined
        process !buf is (I.Partial k) = do s0 <- liftIO $ S.read is
                                           case s0 of 
                                               Just s -> process buf is (k s)
                                               Nothing -> process buf is (k B.empty)
        process !buf is (I.Some xs k) = do let buf' = buf V.++ (V.reverse . V.fromList . rights $ xs)
                                           go buf' is k


connectDelay :: Int -> S.InputStream a -> S.OutputStream a -> IO ()
connectDelay delay p q = loop
    where
        loop = do
            m <- S.read p
            threadDelay delay
            maybe (S.write Nothing q) (const $ S.write m q >> loop) m
{-# INLINE connectDelay #-}


transformToPureText :: Record -> IO (B.ByteString)
transformToPureText ss = do
    case runParser $ parseRecord $ ss of
        Left err -> return ""
        Right t -> do let bs = B.intercalate "," [ _date t
                                                  , _time t
                                                  , B.concat . LB.toChunks . SB.show $ _price t
                                                  , B.concat . LB.toChunks . SB.show $ _bid t
                                                  , B.concat . LB.toChunks . SB.show $ _ask t
                                                  , B.concat . LB.toChunks . SB.show $ _volume t
                                                  ]
                      return $ B.append bs "\n"


transformToProtobuf :: Record -> IO (B.ByteString)
transformToProtobuf ss = do
    case runParser $ parseRecord $ ss of 
        Left err -> return ""
        Right t -> do let tpb = TickProtobuf { pDate = P.putField $ _date t
                                             , pTime = P.putField $ _time t
                                             , pPrice = P.putField $ _price t
                                             , pBid = P.putField $ _bid t
                                             , pAsk = P.putField $ _ask t
                                             , pVolume = P.putField $ _volume t
                                             }
                      return $ P.runPut $ P.encodeMessage tpb


processRequest :: Socket -> SockAddr -> FilePath -> String -> IO ()
processRequest connsock clientaddr tick_filepath serialize_format = do 
    (net_is, net_os) <- S.socketToStreams connsock
    S.withFileAsInput tick_filepath (\is -> do
            is' <- Main.decodeWith I.defaultDecodeOptions False is
            show_is <- (case serialize_format of 
                            "puretext" -> S.mapM transformToPureText is'
                            "protobuf" -> S.mapM transformToProtobuf is'
                            _ -> S.mapM transformToPureText is'
                       )
            
            delay <- R.randomRIO (10, 500) 
            connectDelay delay show_is net_os
        )


launchServer :: ServiceName -> FilePath -> String -> IO ()
launchServer server_port tick_filepath serialize_format = withSocketsDo $ do
    addrinfos <- getAddrInfo (Just (defaultHints {addrFlags = [AI_PASSIVE]})) Nothing (Just server_port)
    let serveraddr = head addrinfos
    sock <- socket (addrFamily serveraddr) Stream defaultProtocol
    bindSocket sock (addrAddress serveraddr)
    listen sock 5

    forever $ do
        (connsock, clientaddr) <- accept sock
        forkFinally (processRequest connsock clientaddr tick_filepath serialize_format) (\_ -> return ())


forkFinally :: IO a -> (Either SomeException a -> IO ()) -> IO ThreadId
forkFinally action and_then =
    mask $ \restore ->
        forkIO $ try (restore action) >>= and_then


data Opts = Opts
    { debug :: Bool
    , sourceFiles :: [FilePath]
    , logFile :: FilePath
    , port :: String
    , serializeFormat :: String
    } deriving (Data, Typeable, Show, Eq)


progOpts :: Opts
progOpts = Opts
    { sourceFiles = def &= args &= typ "source files"
    , port = def &= help "server port"
    , serializeFormat = def &= help "serialize format"
    , debug = def &= help "debug mode"
    , logFile = def &= help "log filepath"
    }


getOpts :: IO Opts
getOpts = cmdArgs $ progOpts
    &= summary (_PROGRAM_INFO ++ ", " ++ _COPYRIGHT)
    &= program _PROGRAM_NAME
    &= help _PROGRAM_DESC
    &= helpArg [explicit, name "help", name "h"]
    &= versionArg [explicit, name "version", name "v", summary _PROGRAM_INFO]


_PROGRAM_NAME :: String
_PROGRAM_NAME = "ycombitrader-mock-server"

_PROGRAM_VERSION :: String
_PROGRAM_VERSION = "0.0.1"

_PROGRAM_INFO :: String
_PROGRAM_INFO = _PROGRAM_NAME ++ " version " ++ _PROGRAM_VERSION

_PROGRAM_DESC :: String
_PROGRAM_DESC = "a mocking stock tick server"

_COPYRIGHT :: String
_COPYRIGHT = "BSD-3"


main :: IO ()
main = do 
    opts <- getOpts

    if (not . null . sourceFiles) opts 
        then do 
            let filename = head . sourceFiles $ opts
                server_port = port $ opts
                serialize_format = serializeFormat $ opts
            logger <- mkLogger True stdout
            loggerPutStr logger $ map toLogStr ["reading file ", filename, "\n"]
            loggerPutStr logger $ map toLogStr ["serialize format: ", serialize_format, "\n"]
            launchServer server_port filename serialize_format
            return ()
        else do putStrLn $ "No input file given."
