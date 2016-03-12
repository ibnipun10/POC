var constants = require('./constants.js');

module.exports = {
    convertToJson: function (validparams) {
        return JSON.stringify(validparams)
    },
    
    convertTocsv : function (params) {
        return params.toString();
    },

    randomNum : function (){
        return Math.floor(Math.random() * constants.RANDOM_LIMIT);
    }
}