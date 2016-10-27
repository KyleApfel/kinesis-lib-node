'use strict';
const
  R         = require ('ramda'),
  T         = require ('data.task'),
  M         = require ('control.monads'),

  logI      = function (x) { console.log (x); return x; },
  id        = function (x) { return x; },
  konst     = R.curry (function (a, b) { return a; }),
  du        = function (M) { return function () { return R.apply (R.pipe, arguments)(M.of ({})); }; },
  bind      = function (a) { return R.chain (konst (a)); },
  chain     = R.chain,
  map       = R.map,

//Misc Modules
  AWS       = require('aws-sdk'),

//putRecords :: KinesisConfObj -> KinesisParamsObj -> Task(KinesisPutResObj)
  putRecords = R.curry (function (conf, params) {
    var kinesis = new AWS.Kinesis(conf);
    return new T (function (reject, resolve) {
      kinesis.putRecords(params, R.curry(function (e, d) {
        if (e) resolve(logI(e))
               resolve(d)
      }))
    });
  }),

//wrapPutRecordsData :: String -> String -> Obj
  wrapPutRecordsData = R.curry (function (conf, partition, data) {
    var obj = {Data: JSON.stringify(data), PartitionKey: partition}
    if (R.isNil(conf.ExplicitHashKey) === false) { return R.assoc('ExplicitHashKey', conf.ExplicitHashKey, obj); } 
                                                   return obj;
  }),

//wrapInX :: String -> Obj -> Obj
  wrapInX = R.curry (function (key, obj) {
    return R.assoc(key, obj, {}) 
  }),

//putRecordsT :: KinesisConfObj -> String -> String -> [a] -> Task a
  putRecordsT = R.curry (function (conf, streamName, partition, data) {
    return du (T) ( bind  (T.of (data))
                  , map   (map (wrapPutRecordsData(conf, partition)))
                  , map   (wrapInX ('Records'))
                  , map   (R.assoc ('StreamName', streamName))
                  , chain (putRecords (conf))
                  , map   (konst (data))
                  , id );
  }),

  nil       = null;

/* Example Usage
*putRecordsT({region: 'us-east-1'}, "ExampleStream", 'ExamplePartition', [{"ExampleKey" : "ExampleData1" }
*                                                                        ,{"ExampleKey" : "ExampleData2" }
*                                                                        ,{"ExampleKey" : "ExampleData3" }
*                                                                        ,{"ExampleKey" : "ExampleData4" }
*                                                                        ,{"ExampleKey" : "ExampleData5" } ]).fork(logI, logI) 
*/
module.exports = putRecordsT
