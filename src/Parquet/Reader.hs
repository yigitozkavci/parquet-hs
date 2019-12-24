{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE TypeApplications #-}

module Parquet.Reader where

import Data.Maybe (fromMaybe)
import Data.Foldable (traverse_)
import Data.Traversable (for)
import qualified Data.Conduit.List as CL
import qualified Data.Conduit.Binary as CB
import Control.Monad.Logger (MonadLogger, runNoLoggingT)
import Control.Monad.Logger.CallStack (logWarn, logInfo, logError)
import Data.Functor ((<$))
import Parquet.Stream.Reader
  (Value(..), readColumnChunk, ColumnValue(..), decodeConduit)
import Control.Lens hiding (ix)
import Control.Monad.Reader (runReaderT, MonadReader, ask)
import qualified Data.Map as M
import Control.Arrow ((&&&))
import qualified Data.Text as T
import qualified Data.HashMap.Strict as HM
import Control.Monad.Except
import Control.Monad (foldM)
import qualified Data.Sequence as Seq

import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as BS8
import qualified Conduit as C
import System.IO
  (IOMode(ReadMode), openFile, SeekMode(AbsoluteSeek, SeekFromEnd), hSeek)
import Network.HTTP.Types.Status (statusIsSuccessful)
import Network.HTTP.Simple
  (getResponseBody, getResponseStatus, httpSource, parseRequest, Header)
import Network.HTTP.Client (Request(requestHeaders))
import qualified Parquet.ThriftTypes as TT
import Parquet.Utils (failOnExcept)
import qualified Data.Binary.Get as BG
import Parquet.ParquetObject

newtype ParquetSource m = ParquetSource (Integer -> C.ConduitT () BS.ByteString m ())

type Url = String

readFieldTypeMapping
  :: MonadError T.Text m => TT.FileMetadata -> m (HM.HashMap T.Text TT.Type)
readFieldTypeMapping fm =
  let schemaElements = fm ^. TT.pinchField @"schema"
  in
    fmap HM.fromList $ for schemaElements $ \se -> do
      let name = se ^. TT.pinchField @"name"
      case se ^. TT.pinchField @"type" of
        Nothing ->
          throwError $ "Type info for field " <> name <> " doesn't exist"
        Just ty -> pure (name, ty)

readMetadata
  :: (MonadError T.Text m, MonadIO m) => ParquetSource m -> m TT.FileMetadata
readMetadata (ParquetSource source) = do
  bs <- C.runConduit (source (-8) C..| CB.take 8)
  case BG.runGetOrFail BG.getWord32le bs of
    Left err -> fail $ "Could not fetch metadata size: " <> show err
    Right (_, _, metadataSize) ->
      fmap (snd . fst)
        $            C.runConduit
        $            source (-(8 + fromIntegral metadataSize))
        C..|         decodeConduit metadataSize
        `C.fuseBoth` pure ()

localParquetFile :: C.MonadResource m => FilePath -> ParquetSource m
localParquetFile fp = ParquetSource $ \pos -> C.sourceIOHandle $ do
  h <- openFile fp ReadMode
  if pos > 0 then hSeek h AbsoluteSeek pos else hSeek h SeekFromEnd pos
  pure h

remoteParquetFile
  :: (C.MonadResource m, C.MonadThrow m, C.MonadIO m) => Url -> ParquetSource m
remoteParquetFile url = ParquetSource $ \pos -> do
  req <- parseRequest url
  let
    rangedReq = req { requestHeaders = mkRangeHeader pos : requestHeaders req }
  httpSource rangedReq call
 where
  mkRangeHeader :: Integer -> Header
  mkRangeHeader pos =
    let rangeVal = if pos > 0 then show pos <> "-" else show pos
    in ("Range", "bytes=" <> BS8.pack rangeVal)

  call req =
    let status = getResponseStatus req
    in
      if statusIsSuccessful status
        then getResponseBody req
        else
          fail
          $  "Non-success response code from remoteParquetFile call: "
          ++ show status

readWholeParquetFile
  :: ( C.MonadThrow m
     , MonadIO m
     , MonadError T.Text m
     , C.MonadResource m
     , MonadLogger m
     )
  => String
  -> m [ParquetValue]
readWholeParquetFile inputFp = do
  metadata <- readMetadata (localParquetFile inputFp)
  (`runReaderT` metadata)
    $    C.runConduit
    $    traverse_
           (sourceRowGroup (localParquetFile inputFp))
           (metadata ^. TT.pinchField @"row_groups")
    C..| CL.consume

type Record = [(ColumnValue, [T.Text])]

sourceParquet :: FilePath -> C.ConduitT () ParquetValue (C.ResourceT IO) ()
sourceParquet fp = runExceptT (readMetadata (localParquetFile fp)) >>= \case
  Left err -> fail $ "Could not read metadata: " <> show err
  Right metadata ->
    C.transPipe (runNoLoggingT . (`runReaderT` metadata)) $ traverse_
      (sourceRowGroup (localParquetFile fp))
      (metadata ^. TT.pinchField @"row_groups")

foldMaybeM
  :: (Foldable t, Monad m) => (b -> a -> m (Maybe b)) -> b -> t a -> m b
foldMaybeM action = foldM $ \b a -> action b a >>= \case
  Nothing   -> pure b
  Just newB -> pure newB


sourceRowGroupFromRemoteFile
  :: ( C.MonadResource m
     , C.MonadIO m
     , C.MonadThrow m
     , MonadLogger m
     , MonadReader TT.FileMetadata m
     )
  => String
  -> TT.RowGroup
  -> C.ConduitT () ParquetValue m ()
sourceRowGroupFromRemoteFile url rg = sourceRowGroup (remoteParquetFile url) rg

throwOnNothing :: MonadError err m => err -> Maybe a -> m a
throwOnNothing err Nothing  = throwError err
throwOnNothing _   (Just v) = pure v

initColumnState :: ParquetValue
initColumnState = ParquetObject $ MkParquetObject mempty

-- Instruction generator for a single column.
--
-- In a parquet column, a repetition level of 0 denotes start of a new record.
-- Example:
--
-- ________________________________________________________
-- | rep_level | path                             | value | 
-- |___________|__________________________________|_______|
-- | 0         | f1, list, element, list, element | 1     |
-- | 2         | f1, list, element, list, element | 2     |
-- | 1         | f1, list, element, list, element | 3     |
-- | 2         | f1, list, element, list, element | 4     |
-- | 1         | f1, list, element, list, element | 4     |
-- | 0         | f1, list, element, list, element | 1     |
-- | 2         | f1, list, element, list, element | 2     |
-- | 2         | f1, list, element, list, element | 3     |
-- |___________|__________________________________|_______|
--
-- Consuming the stream above will yield the following two @ColumnConstructor@s:
--
-- ___________________________________
-- | { "f1": [[1, 2], [3, 4], [5]] } |
-- | { "f1": [[1, 2, 3]]           } |
-- |_________________________________|
generateInstructions
  :: forall m
   . ( C.MonadResource m
     , C.MonadIO m
     , C.MonadThrow m
     , MonadLogger m
     , MonadReader TT.FileMetadata m
     )
  => C.ConduitT (ColumnValue, [T.Text]) ColumnConstructor m ()
generateInstructions = loop Seq.empty
 where
  loop
    :: Seq.Seq InstructionSet
    -> C.ConduitT (ColumnValue, [T.Text]) ColumnConstructor m ()
  loop instructions = C.await >>= \case
    Nothing ->
      unless (Seq.null instructions) $ C.yield $ ColumnConstructor instructions
    Just cv@(ColumnValue { _cvRepetitionLevel }, _) ->
      case (_cvRepetitionLevel, instructions) of
        (0, Seq.Empty) -> go Seq.empty cv
        (0, _        ) -> do
          C.yield $ ColumnConstructor instructions
          C.leftover cv
          loop Seq.empty
        (_, Seq.Empty) ->
          logError
            "generateInstructions: Repetition level is nonzero but we don't have any accumulated instructions. This either means there is a bug in this logic or record is corrupted."
        _ -> go instructions cv

  go
    :: Seq.Seq InstructionSet
    -> (ColumnValue, [T.Text])
    -> C.ConduitT (ColumnValue, [T.Text]) ColumnConstructor m ()
  go ix cv = mkInstructions cv >>= \case
    Nothing -> logError "Could not create instructions: "
    Just is -> loop (ix Seq.|> is)

-- | Given a single column, generates instructions for how to build an object with that column.
--
-- For example, for the following column:
--(ColumnValue {_cvRepetitionLevel = 0, _cvDefinitionLevel = 5, _cvMaxDefinitionLevel = 5, _cvValue = ValueInt64 1},["arr2","list","element","list","element"])
-- [ IObjectField "arr2"
-- , INewList
-- , INewList
-- , IValue (ValueInt64 1)
-- ]
--
-- See the following blog post to understand:
-- https://blog.twitter.com/engineering/en_us/a/2013/dremel-made-simple-with-parquet.html
mkInstructions
  :: forall m
   . ( C.MonadResource m
     , C.MonadIO m
     , C.MonadThrow m
     , MonadLogger m
     , MonadReader TT.FileMetadata m
     )
  => (ColumnValue, [T.Text])
  -> m (Maybe InstructionSet)
mkInstructions = go 1
 where
  go currListLevel c = do
    logInfo $ "Creating instruction for column value: " <> T.pack (show c)
    case c of
      (ColumnValue _ 0 _ v, []) -> pure $ Just $ Seq.singleton $ IValue v
      (ColumnValue{}      , []) -> Nothing
        <$ logWarn "Saw column with nonzero rep/def levels and empty path."
      (ColumnValue r d md v, "list" : "element" : restPath)
        | d == 0 -> do
          when (v /= Null)
            $ logWarn
                "Definition level is zero, path is nonempty but we have a non-null value."
          pure $ Just $ Seq.singleton INullList
        | r == 0 || currListLevel >= r -> do
          mb_rest_instructions <- go
            (currListLevel + 1)
            (ColumnValue r (d - 2) md v, restPath)
          pure $ (INewList Seq.<|) <$> mb_rest_instructions
        | otherwise -> do
          mb_rest_instructions <- go
            (currListLevel + 1)
            (ColumnValue r (d - 2) md v, restPath)
          pure $ (IListElement Seq.<|) <$> mb_rest_instructions
      (ColumnValue r d md v, fieldName : restPath)
        | d == 0 -> do
          when (v /= Null)
            $ logWarn
                "Definition level is zero, path is nonempty but we have a non-null value."
          pure $ Just $ IObjectField fieldName Seq.<| Seq.singleton INullObject
        | otherwise -> do
          mb_rest_instructions <- go
            currListLevel
            (ColumnValue r (d - 1) md v, restPath)
          pure $ (IObjectField fieldName Seq.<|) <$> mb_rest_instructions

newtype ColumnConstructor = ColumnConstructor
  { ccInstrSet :: Seq.Seq InstructionSet
  } deriving (Eq, Show)

-- | Streams the values for every column chunk and zips them into records.
--
-- Illustration:
--
-- _____________________
-- | col1 | col2 | col3 |
-- |  1   |   a  |   x  |
-- |  2   |   b  |   y  |
-- |  3   |   c  |   z  |
-- |______|______|______|
--
-- @sourceRowGroup@ yields the following values in a stream:
--
-- (1, a, x)
-- (2, b, y)
-- (3, c, z)
sourceRowGroup
  :: forall m
   . ( C.MonadResource m
     , C.MonadIO m
     , C.MonadThrow m
     , MonadLogger m
     , MonadReader TT.FileMetadata m
     )
  => ParquetSource m
  -> TT.RowGroup
  -> C.ConduitT () ParquetValue m ()
sourceRowGroup source rg = do
  logInfo "Parsing new row group."
  C.sequenceSources
      (map
        (\cc ->
          sourceColumnChunk source cc
            C..| CL.mapMaybe ((<$> mb_path cc) . (,))
            C..| generateInstructions
        )
        (rg ^. TT.pinchField @"column_chunks")
      )
    C..| CL.mapM (construct_record initColumnState)
 where
  mb_path :: TT.ColumnChunk -> Maybe [T.Text]
  mb_path cc =
    TT.unField
      .   TT._ColumnMetaData_path_in_schema
      <$> (cc ^. TT.pinchField @"meta_data")

  construct_record :: ParquetValue -> [ColumnConstructor] -> m ParquetValue
  construct_record = foldM construct_column

  construct_column :: ParquetValue -> ColumnConstructor -> m ParquetValue
  construct_column pv = foldM apply_instructions pv . ccInstrSet

  apply_instructions :: ParquetValue -> InstructionSet -> m ParquetValue
  apply_instructions val instrSet =
    runExceptT (interpretInstructions val instrSet) >>= \case
      Left err -> ParquetNull
        <$ logError ("Error while interpreting instructions: " <> err)
      Right newVal -> pure newVal

valueToParquetValue :: Value -> ParquetValue
valueToParquetValue Null                 = ParquetNull
valueToParquetValue (ValueInt64      v ) = ParquetInt v
valueToParquetValue (ValueByteString bs) = ParquetString bs

type InstructionSet = Seq.Seq Instruction

data Instruction =
  IValue Value
  | IListElement
  | INewList
  | INullList
  | INullObject
  | IObjectField T.Text
  deriving (Eq, Show)


-- | Traverses through given instruction list and changes the given ParquetValue accordingly.
--
-- Given;
-- Value: {}
-- Instruction Set: [IObjectField "f1",INewList,INewList,IValue (ValueInt64 1)]
--
-- Returns;
-- { "f1": [[1]] }
--
-- Given;
-- Value: { "f1": [[1]] }
-- Instruction Set: [IObjectField "f1",IListElement,INewList,IValue (ValueInt64 2)]
--
-- Returns;
-- { "f1": [[1, 2]] }
interpretInstructions
  :: (MonadLogger m, MonadError T.Text m)
  => ParquetValue
  -> InstructionSet
  -> m ParquetValue
interpretInstructions parquetVal is = do
  logInfo
    $  "Interpreting instructions: "
    <> T.pack (show parquetVal)
    <> ", "
    <> T.pack (show is)
  case (parquetVal, is) of
    (EmptyValue, Seq.Empty) ->
      throwError "Could not generate a parquet value with given instructions."
    (ParquetNull, _           ) -> pure ParquetNull
    (pv         , Seq.Empty   ) -> pure pv
    (pv         , i Seq.:<| ix) -> case i of
      IValue val   -> pure $ valueToParquetValue val
      INullList    -> pure $ ParquetList $ MkParquetList []
      INullObject  -> pure $ ParquetObject $ MkParquetObject $ HM.fromList []
      IListElement -> case pv of
        ParquetList (MkParquetList xs) -> case reverse xs of
          (revX : revXs) -> do
            newRevX <- interpretInstructions revX ix
            pure $ ParquetList $ MkParquetList $ reverse $ newRevX : revXs
          _ -> throwError "List is empty for NestedListElement instruction"
        v ->
          throwError
            $  "Wrong parquet value "
            <> T.pack (show v)
            <> " type for instruction IListElement"
      INewList -> case pv of
        EmptyValue -> do
          newX <- interpretInstructions EmptyValue ix
          pure $ ParquetList $ MkParquetList [newX]
        ParquetList (MkParquetList xs) -> do
          newX <- interpretInstructions EmptyValue ix
          pure $ ParquetList $ MkParquetList $ xs <> [newX]
        v ->
          throwError
            $  "Wrong parquet value "
            <> T.pack (show v)
            <> " type for instruction INewList"
      IObjectField fieldName -> case pv of
        EmptyValue -> do
          val <- interpretInstructions EmptyValue ix
          pure $ ParquetObject $ MkParquetObject $ HM.fromList
            [(fieldName, val)]
        ParquetObject (MkParquetObject hm) -> do
          newObj <- flip (at fieldName) hm $ \mbExistingParquetVal ->
            Just
              <$> interpretInstructions
                    (fromMaybe EmptyValue mbExistingParquetVal)
                    ix
          pure $ ParquetObject $ MkParquetObject newObj
        v ->
          throwError
            $  "Cannot apply IObjectField instruction on parquet value "
            <> T.pack (show v)

sourceColumnChunk
  :: ( MonadReader TT.FileMetadata m
     , C.MonadIO m
     , C.MonadResource m
     , C.MonadThrow m
     , MonadLogger m
     , MonadReader TT.FileMetadata m
     )
  => ParquetSource m
  -> TT.ColumnChunk
  -> C.ConduitT () ColumnValue m ()
sourceColumnChunk (ParquetSource source) cc = do
  metadata <- ask
  let
    schema_mapping =
      M.fromList
        $  map ((^. TT.pinchField @"name") &&& id)
        $  metadata
        ^. TT.pinchField @"schema"
  let offset = cc ^. TT.pinchField @"file_offset"
  source (fromIntegral offset)
    C..| C.transPipe failOnExcept (readColumnChunk schema_mapping cc)
