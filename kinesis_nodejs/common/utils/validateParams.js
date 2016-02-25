var cassandra = require('../../server/connectors/cassandra.js'),
    constants = require('./constants.js'),
    cacheHelper = require('./cacheHelper.js'),
    utils = require('../utils/util.js'),
    moment = require('moment'),

    acceptedDateRange = {
        min: 7 * 24 * 60 * 60,  // One week ago
        max: 4 * 60 * 60        // 4 hours from now
    };

module.exports = {
    
    // Is this event name in the list of valid events?
    eventName: function (thisEventName, next) {
        if (thisEventName == null || thisEventName == undefined) {
            next(false, null);
            return;
        }
        
        thisEventName = thisEventName.toLowerCase();
        cassandra.validateData.eventName(thisEventName, next);
    },
    
    // Is this a valid combination of API key and APP name?
    appNameKey: function (thisKey, appName, next) {
        if (thisKey == null || thisKey == undefined || utils.isNullorEmpty(appName)) {
            next(false);
            return;
        }
        
        appName = appName.toLowerCase();
        cassandra.validateData.key(thisKey, appName, next);
    },    
    
    // Is the event time recent, and not in the future?
    eventTime: function (thisEventTime) {
        try {
            if (thisEventTime == null || thisEventTime == undefined)
                return false;
            
            var validdate = moment(thisEventTime);
            if (validdate != null && validdate.isValid()) {
                
                var diff = validdate.diff(moment(new Date()).utc(), 's');
                return (
	                diff <= acceptedDateRange.max && diff >= -acceptedDateRange.min
                    );
            }
        }
            catch (e) {  //error in reading date, return a false here
            console.log(e);    
        }

        return false;
    },
    
    // Is the timezone within +/- 12 hours of UTC?
    timeZone: function (thisTimeZone) {
        return Math.abs(Number(thisTimeZone)) <= 12;
    },
    
    // Does the meta data contain all the correct dat for this type of event?
    metaAndRequired: function (thisEventName, params, next) {
        thisEventName = thisEventName.toLowerCase();
        cassandra.validateData.metaAndRequired(thisEventName, params, next);
    },
    
    //check for valid format for meta or required
    checkValidFormat: function (input){
        if (utils.isNullorEmpty(input))
            input = '{}';
        
        try {
            var json = utils._mapToJSON(input);
        
            JSON.parse(json);
            return true;
        } catch (e) {
            return false;
        }
    },

    //validate the authorization key
    checkAuthorizationKey: function (key) {
        if (key == constants.AUTHORIZING_KEY)
            return true;
        else
            return false;
    },

    validateKey: function (ctx) {
        key = ctx.req.get(constants.HEADER_AUTHORIZATION);
        if (module.exports.checkAuthorizationKey(key))
            return true;
        else
            throwError.unauthorized(ctx, 302, constants.ERROR_INVALID_KEY);
    
        return false;
    },

    // Is this a valid App name?
    appName: function (appName, next) {
        if (appName == null || appName == undefined) {
            next(false);
            return;
        }
        
        appName = appName.toLowerCase();        
        cassandra.validateData.appname(appName, next);
    },

    validateSerials: function (params) {
        var requiredffields = cacheHelper.getValue(params.eventName, constants.CACHEREQUIRED);
        var assetSerial = params.assetSerial;
        var serialsCount = 0;
        
        if (requiredffields.length > 0) {
            for (var i = 0; i < requiredffields.length; i++) {
                if (requiredffields[i].substring(0, constants.SERIALS.length) == constants.SERIALS) {
                    serialsCount++;
                }
            }
            
            if (assetSerial != undefined && assetSerial != null) {
                var breturn = true;
                var splitter = assetSerial.split(constants.SERIALS_DELIMITER);
                if (splitter.length >= serialsCount){                           
                    for (var i = 0; i < serialsCount; i++) {
                        if (utils.isNullorEmpty(splitter[i])) {
                            breturn = false;
                            break;
                        }
                            
                    }               
                    
                    if(breturn)
                        return [serialsCount, true];
                }
            }
            
                
            //validate serials fields here and return false. For invalid serials throw an exception
            for (var i = 0; i < requiredffields.length; i++) {
                var splitdot = requiredffields[i].split('.');
                if (splitdot.length > 1) {
                    var paramcopy = params;
                    for (var j = 0; j < splitdot.length; j++) {
                        var value = paramcopy[splitdot[j]];
                        if (value == undefined || value == null || utils.isNullorEmpty(value)) {
                            //throw exception here 
                            throw "Invalid serials";
                            //next(false, constants.VALIDATION_TYPE_REQUIREDFIELDS);                                
                        }
                        else {
                            try {
                                if (j != splitdot.length - 1)
                                    paramcopy = JSON.parse(utils._mapToJSON(value));
                            } catch (e) { }

                        }
                    }

                }
            }
            return [serialsCount,false];
            
        }

        return [serialsCount,true];
    }
};