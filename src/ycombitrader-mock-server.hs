{-# LANGUAGE OverloadedStrings, DeriveDataTypeable, DeriveGeneric, BangPatterns #-}

module Main where

import Control.Monad
import Control.Monad.IO.Class (liftIO)
import Control.Concurrent
import Control.Exception

import qualified Data.ByteString as B
import qualified Data.ByteString.Lex.Double as B
import qualified Data.ByteString.Lex.Integral as B
import qualified Data.ProtocolBuffers as P
import Data.TypeLevel (D1, D2, D3)
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
import System.Log.FastLogger
import qualified System.IO.Streams.File as S
import qualified System.IO.Streams as S


type Price = Double
type Volume = Int

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


data Foo = Foo
   { field1 :: P.Required D1 (P.Value Bool) -- ^ The last field with tag = 1
   } deriving (Generic, Show)

instance P.Encode Foo
instance P.Decode Foo


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


processRequest :: Socket -> SockAddr -> FilePath -> IO ()
processRequest connsock clientaddr tick_filepath= do 
    (net_is, net_os) <- S.socketToStreams connsock
    S.withFileAsInput tick_filepath (\is -> do
            is' <- Main.decodeWith I.defaultDecodeOptions False is
            show_is <- S.mapM_ (\ss -> do
                                    case runParser $ parseRecord $ ss of
                                        Left err -> putStrLn . show $ err
                                        Right t -> putStrLn . show $ _price t
                                    return $ "GG"
                                ) is'
            S.connect is net_os
        )


launchServer :: ServiceName -> FilePath -> IO ()
launchServer server_port tick_filepath = withSocketsDo $ do
    addrinfos <- getAddrInfo (Just (defaultHints {addrFlags = [AI_PASSIVE]})) Nothing (Just server_port)
    let serveraddr = head addrinfos
    sock <- socket (addrFamily serveraddr) Stream defaultProtocol
    bindSocket sock (addrAddress serveraddr)
    listen sock 5

    forever $ do
        (connsock, clientaddr) <- accept sock
        forkFinally (processRequest connsock clientaddr tick_filepath) (\_ -> return ())


forkFinally :: IO a -> (Either SomeException a -> IO ()) -> IO ThreadId
forkFinally action and_then =
    mask $ \restore ->
        forkIO $ try (restore action) >>= and_then


data Opts = Opts
    { debug :: Bool
    , sourceFiles :: [FilePath]
    , logFile :: FilePath
    , port :: String
    } deriving (Data, Typeable, Show, Eq)


progOpts :: Opts
progOpts = Opts
    { sourceFiles = def &= args &= typ "source files"
    , port = def &= help "server port"
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
            logger <- mkLogger True stdout
            loggerPutStr logger $ map toLogStr ["reading file ", filename, "\n"]
            launchServer server_port filename
            return ()
        else do putStrLn $ "No input file given."
