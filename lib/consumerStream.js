'use strict';
const
  R         = require ('ramda'),
  T         = require ('data.task'),
  M         = require ('control.monads'),

  logI      = function (x) { console.log (x); return x; },
  id        = function (x) { return x; },
  konst     = R.curry (function (a, b) { return a; }),
  du        = function (M) { return function () { return R.apply (R.pipe, arguments) (M.of ({})); }; },
  bind      = function (a) { return R.chain (konst (a)); },
  chain     = R.chain,
  map       = R.map,
  ap        = R.curry (function (ma, mf) { return mf.ap (ma); }),
  random    = R.curry (function (min, max) { return Math.floor (Math.random () * (max - min + 1)) + min; }),
  runTaskIO = R.curry (function (rej, res, t) { return IO (function () { t.fork (rej, res); }); }),

//Misc Modules
  AWS       = require ('aws-sdk'),
  atob      = require ('atob'),

//getShardIterator :: KinesisConfObj -> KinesisParamsObj -> ShardIteratorObj
  getShardIterator = R.curry (function (kConf, params) {
    var kinesis = new AWS.Kinesis (kConf);
    return new T (function (reject, resolve) {
      kinesis.getShardIterator (params, R.curry (function (e, d){
        if (e) reject (logI (e))
               resolve (d)
      }))
    });
  }),

//shardIteratorObj2GetRecordsObj :: Int -> ShardIteratorObj -> KinesisParamsObj
  shardIteratorObj2GetRecordsObj = R.curry (function (amount, obj) {
    var shardIterator = obj.ShardIterator 
    return generateGetRecordsObj (amount, shardIterator)
  }),

//nextShardIterator2GetRecordsObj :: Int -> KinesisResObj -> KinesisParamsObj
  nextShardIterator2GetRecordsObj = R.curry (function (amount, obj) {
    var nextShardIterator = obj.NextShardIterator
    return generateGetRecordsObj (amount, nextShardIterator)
  }),

//generateGetRecordsObj :: ShardIteratorObj -> KinesisParamsObj -> Int -> KinesisParamsObj
  generateGetRecordsObj = R.curry (function (amount, shardIterator)  {
    return R.compose (R.assoc ('ShardIterator', shardIterator)
                     ,R.assoc ('Limit'        , amount       )
                     ) ({})
  }),

//getRecords :: KinesisConfObj -> KinesisParamsObj -> KinesisResObj
  getRecords = R.curry (function (kConf, params) {
    var kinesis = new AWS.Kinesis (kConf);
    return new T (function (reject, resolve) {
      kinesis.getRecords (params, R.curry (function (e, d) {
        if (e) reject (logI (e))
               resolve (d)
      }))      
    });
  }),

//keyDataContains :: String -> String -> Obj -> Bool
  keyDataContains = R.curry (function (key, data, obj) {
    var xLens = R.lensProp (key)
    if (data === '') return true;
    return ( (R.view (xLens, obj)) === data)
  }),

//filterPartition :: String -> Obj -> Obj
  filterPartition = R.curry (function (string, obj) {
    return R.assoc ('Records', R.filter (keyDataContains ("PartitionKey", string), obj.Records), obj)
  }),

//debufferData :: KinesisDataObj -> KinesisDataObj
  debufferData = R.curry (function (obj) {
    var data = atob (obj.Data);
    return R.assoc ('Data', data, obj)
  }),

//debufferKinesisData :: KinesisResObj -> KinesisResObj
  debufferKinesisData = R.curry (function (obj) {
    return R.assoc ('Records', R.map (debufferData, obj.Records), obj)
  }),

//holdForX :: Int -> a -> a
  holdForX = R.curry (function (time, obj) {
    return new T (function (reject, resolve) {
      setTimeout (function () {
        resolve (obj) 
      }, time);
    });
  }),

//logIRecordData :: KinesisRecordsObj 
  logIRecordData = function (obj) {
    logI (obj.Data)
  },

/*
***********************
**getSharedIteratorObj*
***********************
* {
*    "StreamName"             : String,        -- Required
*    "ShardId"                : Int :: String, -- Required
*    "ShardIteratorType"      : iteratorType,  -- Required
*    "StartingSequenceNumber" : String,
*    "Timestamp"              : unixTimestamp | 1969-01-01T01:01:00.100-00:00 :: String
* }
* * 
* iteratorType: AT_SEQUENCE_NUMBER | AFTER_SEQUENCE_NUMBER | TRIM_HORIZON | LATEST | AT_TIMESTAMP :: String
*
*****************
**KinesisConfObj*
*****************
* {
*    "region": regionType, -- Required
*    ...     : ...         -- More Info: http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/Kinesis.html#constructor-property
* }
* 
* regionType: us-east-1 | us-east-2 | us-west-2 | ap-northeast-2 | ap-southeast-1 
*           | ap-southeast-2 | ap-northeast-1 | eu-central-1 | eu-west-1 :: String
*
****************
**KinesisResObj*
****************
* {
*    Records            : [KinesisRecordObj]
*    NextShardIterator  : String,
*    MillisBehindLatest : Int
* }
* 
* KinesisRecordObj
* {
*    SequenceNumber: Int,
*    ApproximateArrivalTimestamp: Mon Jan 01 1969 01:01:00 GMT-0000 (TIMEZONE),
*    Data: String
* }
*/

//getRecordsT :: KinesisConfObj -> getShardIteratorObj -> String -> Int -> (a -> M a) -> KinesisResObj
  getRecordsT = R.curry (function (kConf, params, filter, limit, task) {
    return du (T) ( bind  (getShardIterator (kConf, params)) 
                  , map   (shardIteratorObj2GetRecordsObj (limit))
                  , chain (getRecords (kConf))
                  , map   (filterPartition (filter))
                  , map   (debufferKinesisData)
                  , chain (holdForX (5))
                  , chain (task)
                  , chain (getMoreRecordsT (kConf, filter, limit, task))
                  , id );
  }),

//getMoreRecordsT :: KinesisConfObj -> String -> Int -> (a -> M a) -> KinesisResObj
  getMoreRecordsT = R.curry (function (kConf, filter, limit, task, params) {
      return du (T) ( bind  (T.of ( (params)))
                    , map   (nextShardIterator2GetRecordsObj (limit))
                    , chain (getRecords (kConf))
                    , map   (filterPartition (filter))
                    , map   (debufferKinesisData)
                    , chain (holdForX (5))
                    , chain (task)
                    , chain (getMoreRecordsT (kConf, filter, limit, task))
                    , id );
   }),

//confHelper :: String -> String -> KinesisConfObj
  confHelper = R.curry (function (streamName, shardId) {
    return { "StreamName": streamName.toString (), "ShardId": shardId.toString (), "ShardIteratorType": "LATEST" };
  }),

//logIT :: a -> Task (a)
  logIT = R.curry (function (a) {
    return new T (function (reject, resolve) { return resolve (logI (a)) })
  }),

//logIRecordsT :: KinesisResObj -> KinesisResObj
  logIRecordsT = R.curry (function (a) {
    return new T (function (reject, resolve) { 
      R.map (logIRecordData, a.Records)
      return resolve (a) 
    })
  }),

   nil = null;

/* Example Usage
*var confdGetRecords = getRecordsT ({region: 'us-east-1'}
*                                  ,confHelper ("ExampleStream", 0)
*                                  , '8634', 50);
*confdGetRecords (logIRecordsT).fork (id,id)
*/
module.exports = {
                  getRecordsT
                 ,confHelper
                 ,logIT
                 ,logIRecordsT
                 }
