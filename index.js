module.exports = { putRecord    : require ('./lib/putRecord'         )
                 , putRecords   : require ('./lib/putRecords'        )
                 , getRecords   : require ('./lib/consumerStream.js' ).getRecordsT  
                 , confHelper   : require ('./lib/consumerStream.js' ).confHelper   
                 , logIT        : require ('./lib/consumerStream.js' ).logIT        
                 , logIRecordsT : require ('./lib/consumerStream.js' ).logIRecordsT };
