var constants = require('./constants');


var NodeCache = require('node-cache'),
    myCache = new NodeCache(),
    myCacheMeta = new NodeCache(),
    myCacheRequired = new NodeCache();
    myCachettl = new NodeCache();


module.exports = {
    
 addKeyValue: function (key, value, cacheType) {
  switch(cacheType) {
            case constants.CACHEMETA: myCacheMeta.set(key, value, constants.TTL_VALUE);
                break;
            case constants.CACHEREQUIRED: myCacheRequired.set(key, value, constants.TTL_VALUE);
                break;
            case constants.MYCACHE: myCache.set(key, value, constants.TTL_VALUE);
                break;
            case constants.CACHETTL: myCachettl.set(key, value, constants.TTL_VALUE);
                break;
            default: return;
        }
    },
    
getValue: function (key, cacheType) {

 switch(cacheType) {
            case constants.CACHEMETA: return myCacheMeta.get(key);
            case constants.CACHEREQUIRED: return myCacheRequired.get(key);
            case constants.MYCACHE: return myCache.get(key);
            case constants.CACHETTL: return myCachettl.get(key);
            default: return;
        }

        
    },
    
isKeyPresent: function (key, cacheType) {
        return !(module.exports.getValue(key, cacheType) === undefined);
        
    },

deleteKey: function (key, cacheType) {
        switch (cacheType) {
            case constants.CACHEMETA: return myCacheMeta.del(key)
            case constants.CACHEREQUIRED: return myCacheRequired.del(key);
            case constants.MYCACHE: return myCache.del(key);
            case constants.CACHETTL: return myCachettl.del(key);
            default: return;
        }
    }
    

};