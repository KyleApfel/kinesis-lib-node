module.exports = { putRecord    : require ('./lib/putRecord'         )
                 , putRecords   : require ('./lib/putRecords'        )
                 , getRecords   : require ('./lib/consumerStream.js' ).getRecordsT  
                 , confHelper   : require ('./lib/consumerSTream.js' ).confHelper   
                 , logIT        : require ('./lib/consumerSTream.js' ).logIT        
                 , logIRecordsT : require ('./lib/consumerSTream.js' ).logIRecordsT };
