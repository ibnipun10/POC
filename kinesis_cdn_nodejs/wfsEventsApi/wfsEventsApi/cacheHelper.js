var NodeCache = require('node-cache');

var recordsCache = new NodeCache();

module.exports = {
    getKey: function (key){
        return recordsCache.get(key);
    },

    setKey: function (key, value){
        return recordsCache.set(key, value);
    },

    printKeys: function (){
        console.log(recordsCache.keys());
    }
}