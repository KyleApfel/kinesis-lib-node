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

//putRecord :: KinesisConfObj -> KinesisParamsObj -> Task(KinesisPutResObj)
  putRecord = R.curry (function (conf, params) {
    var kinesis = new AWS.Kinesis(conf);
    return new T (function (reject, resolve) {
      kinesis.putRecord(params, R.curry(function (e, d) {
        if (e) resolve(logI(e))
               resolve(d)
      }))
    });
  }),

//wrapPutRecordData :: String -> String -> Obj
  wrapPutRecordData = R.curry (function (conf, stream, partition, data) {
    var obj = {Data : JSON.stringify(data), PartitionKey: partition, StreamName: stream}
    if (R.isNil(conf.ExplicitHashKey) === false) { return R.assoc('ExplicitHashKey', conf.ExplicitHashKey, obj); } 
                                                   return obj;
  }),

//putRecordT :: KinesisConfObj -> String -> String -> a -> Task a
  putRecordT = R.curry (function (conf, streamName, partition, data) {
    return du (T) ( bind  (T.of (data))
                  , map   (wrapPutRecordData (conf, streamName, partition))
                  , chain (putRecord (conf))
                  , map   (konst (data))
                  , id );
  }),

  nil       = null;

/* Example Usage
*putRecordT({region: 'us-east-1', ExplicitHashKey: '0'}, "ExampleStream", 'ExamplePartition'
*          ,{"ExampleKey": "ExampleData"}).fork(logI, logI)
*/
module.exports = putRecordT
