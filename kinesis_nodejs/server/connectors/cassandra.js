var cacheHelper = require('../../common/utils/cacheHelper.js'),
    constants = require('../../common/utils/constants'),
    cassandra = require('cassandra-driver'),
    utils = require('../../common/utils/util.js'),
    loopback = require('loopback'),
    kinesis = require('./kinesis.js')


var client = new cassandra.Client({ contactPoints: constants.CASSANDRA_SERVER, keyspace: constants.EVENTS_KEYSPACE });

module.exports = function () {
    
    var validateMetaAndRequired = function (metaKeys, requiredffields, params, next) {
        try {
            var thisMetaKeys = _getStrObjKeys(params.meta),
                thisSerials = _getStrObjKeys(params.serials),
                thisMetaValues = _getStrObjValues(params.meta),
                thisSerialValues = _getStrObjValues(params.serials),
                metaValid = true,
                serialsValid = true,   
                paramKeys = Object.keys(params);
               
            
            // Validate meta
            if (metaKeys.length > 0) {
                if (thisMetaKeys.length < metaKeys.length) {
                    next(false, constants.VALIDATION_TYPE_META);
                    return false;
                } else {
                    for (var i = 0; i < metaKeys.length; i++) {
                        if (thisMetaKeys.indexOf(metaKeys[i]) < 0) {
                            next(false, constants.VALIDATION_TYPE_META);
                            return false;
                        }
                        else {
                            if (utils.isNullorEmpty(thisMetaValues[metaKeys[i]])) {
                                next(false, constants.VALIDATION_TYPE_META);
                                return false;
                            }
                        }
                    }
                }
            }
            
                        
            // Validate requiredFields
            if (requiredffields.length > 0) {
                for (var i = 0; i < requiredffields.length; i++) {
                    if (requiredffields[i].substring(0, constants.SERIALS.length) == constants.SERIALS) continue;                                       
                    var value = params[requiredffields[i]]
                    if (value == undefined || value == null || utils.isNullorEmpty(value)) {
                        next(false, constants.VALIDATION_TYPE_REQUIREDFIELDS);
                        return false;
                    }                    
                }
            }

        } catch (err) {
            next(false, null);
	    return false;
        }
            
	return true;
    };
    
    var _getStrObjKeys = function (str) {
        try {
            if (!str) return [];
            
            var objJSON = utils._mapToJSON(str);
            
            if (objJSON === '') return [];
            return Object.keys(JSON.parse(objJSON))
        }
        catch (e) {
            throw "Invalid Meta";
        }
    },

        _getStrObjValues = function (str){
        try {
            if (!str) return [];
            
            var objJSON = utils._mapToJSON(str);
            
            if (objJSON === '') return [];
            return JSON.parse(objJSON);
        }
        catch (e) {
            throw "Invalid Meta";
        }
    },

        _checkTable = function (query, params, cacheKey, next) {
            if (cacheHelper.isKeyPresent(cacheKey, constants.MYCACHE) && cacheKey !== null) {
                next(true, null);
                 return;
                
            }
            else {
                client.execute(query, params, { prepare: true },
            function (err, result) {
                    var bpresent = false;
                    
                    if (err){
                        next(false, err);
                        return;
                    }
                    
                    if (result == undefined || result.rows == undefined || result.rows.length == 0 || result.first() == undefined || result.first().count == 0)
                        bpresent = false;
                    else
                        bpresent = true;
                    
                    if (bpresent)
                        cacheHelper.addKeyValue(cacheKey, true, constants.MYCACHE);
                    next(bpresent, null);              
                });
            }
        },

        _checkMetaAndRequired = function (query, eventname, params, next) {
            
            
            if (eventname !== null && cacheHelper.isKeyPresent(eventname, constants.CACHEMETA) && cacheHelper.isKeyPresent(eventname, constants.CACHEREQUIRED)) {
                var metaKeys = cacheHelper.getValue(eventname, constants.CACHEMETA);
                var requiredffields = cacheHelper.getValue(eventname, constants.CACHEREQUIRED);
                
                if(!validateMetaAndRequired(metaKeys, requiredffields, params, next))
                	return;

                next(true, null);
                return;
                
            }
            else {
                
                client.execute(query, [eventname], { prepare: true },
            function (err, result) {
                    try {
                        if (err)
                        {
                            next(false, constants.VALIDATION_TYPE_SERVER);
                            return;
                        }
                        
                        var metaResults = result.first().meta || {},
                            requiredffields = result.first().requiredfields || {},
                            metaKeys = Object.keys(metaResults),
                            requiredffields = Object.keys(requiredffields);
                            ttl = result.first().ttl;
                        
                        //Save a copy in cache
                        cacheHelper.addKeyValue(eventname, metaKeys, constants.CACHEMETA);
                        cacheHelper.addKeyValue(eventname, requiredffields, constants.CACHEREQUIRED);
                        cacheHelper.addKeyValue(eventname, ttl, constants.CACHETTL);

                        if (!validateMetaAndRequired(metaKeys, requiredffields, params, next))
                            return;              
                        
                        
                        next(true, null);

                    } catch (e) {
                        next(false, null);
                    	return;
                    }
                }
               
                );
            }
            
        },

        validateData = {
            key: function (key, appName, next) {
                _checkTable(
                    ["SELECT COUNT(*) FROM ", constants.EVENTS_APIKEY_TBL, " WHERE appkey=? and appname=?"].join(''),
                    [key, appName],
                    [appName, key].join(''),
                    next
                );
            },
            
            appname: function (appName, next){
                _checkTable(
                    ["SELECT appname FROM ", constants.EVENTS_APIKEY_TBL, " WHERE appname=?"].join(''),
                    [appName],
                    appName,
                    next
                );
            },
            
            eventName: function (eventName, next) {
                _checkTable(
                    ["SELECT COUNT(*) FROM ", constants.EVENTS_EVENTNAME_TBL, " WHERE eventName=? "].join(''),
          [eventName],
          eventName,
          next
                );
            },
            
            brandName: function (brandName) {
                _checkTable(
                    ["SELECT COUNT(*) FROM ", constants.EVENTS_BRANDNAME_TBL, " WHERE brandname=? "].join(''),
          [brandName],
          brandName
                );
            },
            
            metaAndRequired: function (eventName, params, next) {
                _checkMetaAndRequired(
                    ["SELECT meta, requiredfields, ttl FROM ", constants.EVENTS_EVENTNAME_TBL, " WHERE eventname=? LIMIT 1 "].join(''),
          eventName,
          params,
          next
                );
            }
        },

        retrieveLog = function (params, callback) {
            
            var appName = params.appName.toLowerCase();

            var cqlQuery = [
                "SELECT  addedtime as addedTime, appname as appName, assetname as assetSerial, brandname as brandSerial, client, eventname as eventName, eventorigin as eventOrigin, eventtime as eventTime, geolocation as geoLocation, location, meta, packname as packSerial, userid as userId FROM",
                constants.EVENTS_EVENTS_TBL,
                "WHERE",
                "appname=? and",
                "randomnum IN",
                utils.getListOfNumbers(),
                "ORDER BY",
                "addedtime",
                "DESC",
                "LIMIT",
                params.perPage].join(' ');
            
            client.execute(
                cqlQuery,
                [appName],
                { prepare: true },
                function (err, result) {
                    callback(err, result);
                    return;
                }
            );
        },


        getErrorLog = function (perPage, next){
            var cqlQuery = [
                "SELECT addedtime, ipaddress, hostname, code, errmsg, reason, useragent FROM",
                constants.EVENTS_ERRORLOGS_TBL,
                "WHERE",
                "randomnum IN",
                utils.getListOfNumbers(),
                "ORDER BY",
                "addedtime",
                "DESC",
                "LIMIT",
                perPage].join(' ');

            client.execute(
                cqlQuery,
                null,
                { prepare: true },
                function (err, result) {
                    next(err, result);
                    return;
                }
            );
        },
        
        saveErrorLogs = function (code, errmsg, reason) {
            if (!constants.ENABLE_ERRORLOGS)
                return;
            
            var ctx = loopback.getCurrentContext().active.http;
            var cqlInsert = [
                "INSERT INTO",
                constants.EVENTS_ERRORLOGS_TBL,
                "(randomnum, addedtime, ipaddress, hostname, code, errmsg, reason, useragent) values(",
                "?,",
                utils.getCurrentTimeStamp(), ",", // addedtime
                "?,", // ipaddress
                "?,",   //hostname
                "?,", //code
                "?,", // errmsg
                "?,", // reason
                "?)" // useragent
            ].join(' ');
            
            client.execute(
                cqlInsert,
                [
                    utils.randomIntInc(constants.RANDOM_LOWER_LIMIT, constants.RANDOM_HIGHER_LIMIT),
                    utils.getIPAddress(ctx),
                    constants.SERVER_HOSTNAME,
                    code,
                    errmsg,
                    reason,
                    utils.getUserAgent(ctx)
                ],
          { prepare : true },
          function (err, result) { });
        },

        saveToEventName = function (params, next){
            
            if (params.meta == undefined)
                params.meta = 'null';
            if (params.requiredfields == undefined)
                params.requiredfields = 'null';
            if (params.ttl == undefined)
                params.ttl = 'null';

            params.eventName = params.eventName.toLowerCase();
            var cqlInsert = ["INSERT INTO ",
            constants.EVENTS_EVENTNAME_TBL,
            "(eventname, meta, requiredfields, ttl) values(",
            "?,",
            params.meta, ",",
            params.requiredfields, ",",
            params.ttl, ")"].join(' ');

            client.execute(
                cqlInsert,
                [params.eventName],
                 { prepare : true },
                   function (err) {
                        next(err);
                } );
        },

        updateToEventName = function (params, next){
            
            params.meta = params.meta || 'null';
            params.requiredfields = params.requiredfields || 'null';
            params.ttl = params.ttl || 'null';
            
            params.eventName = params.eventName.toLowerCase();

            var cqlUpdate = ["UPDATE",
            constants.EVENTS_EVENTNAME_TBL,
            "SET meta=",
            params.meta, ",",
            "requiredfields=",
            params.requiredfields, ",",
            "ttl=",
            params.ttl,
            "WHERE eventname=?"].join(' ');
            
            client.execute(
                cqlUpdate,
                [params.eventName],
                 { prepare : true },
                   function (err) {
                    next(err);
                });
        },

        deleteFromEventName = function (eventName, next){
            
            eventName = eventName.toLowerCase();

            var cqlDelete = ["DELETE FROM ",
            constants.EVENTS_EVENTNAME_TBL,
            "WHERE eventname=?"
            ].join(' ');

            client.execute(
                cqlDelete,
                [eventName],
                { prepare: true },
                function (err) {
                    
                    //delete from cache as well
                    if (!err) {
                        cacheHelper.deleteKey(eventName, constants.CACHEMETA);
                        cacheHelper.deleteKey(eventName, constants.CACHEREQUIRED);
                        cacheHelper.deleteKey(eventName, constants.CACHETTL);
                        cacheHelper.deleteKey(eventName, constants.MYCACHE);
                    }

                    next(err);
                });
            
        },
    
        getEventName = function (eventName, callback) {
            
            eventName = eventName.toLowerCase();
            var cqlQuery = ["SELECT * FROM ",
                constants.EVENTS_EVENTNAME_TBL,
                "WHERE eventname=?"
            ].join(' ');
        
            client.execute(
                    cqlQuery,
                    [eventName],
                    { prepare: true },
                    function (err, result) {
                    callback(err, result);
                });
            
        },

        updateKey = function (appName, newKey, next)
        {
            appName = appName.toLowerCase();

            var cqlQuery = ["UPDATE ", 
                constants.EVENTS_APIKEY_TBL, 
                " SET appkey =",
                "'",newKey,"'",
                " WHERE appName =",
                "'",appName,"'"].join('');

            client.execute(cqlQuery, 
                null, 
                { prepare : true },
                function (err) {
                next(err);
            });
        },


        saveAppNameKey = function (appName, appKey, next){
            
            appName = appName.toLowerCase();
            var cqlInsert = ["INSERT INTO ",
                constants.EVENTS_APIKEY_TBL,
                "(appname, appkey)",
                "values(?,?)"].join(' ');

        client.execute(
            cqlInsert,
            [appName, appKey],
            { prepare : true },
            function (err) {
                next(err);
            });
        },

        deleteAppNameKey = function (appName, next) {
            appName = appName.toLowerCase();

            var cqlDelete = ["DELETE FROM ",
                constants.EVENTS_APIKEY_TBL,
                "WHERE appName=?"
            ].join(' ');
        
            client.execute(
                cqlDelete,
                    [appName],
                    { prepare: true},
                    function (err) {
                    
                    if (!err)
                        cacheHelper.deleteKey(appName, constants.MYCACHE);
                    next(err);
            });
            
        },  

        getAppKey = function (appName, next){
            
            appName = appName.toLowerCase();

            var cqlQuery = ["SELECT * FROM ",
                    constants.EVENTS_APIKEY_TBL,
                    "WHERE appName=?"
            ].join(' ');
        
            client.execute(
                cqlQuery,
                [appName],
                { prepare: true },
                function (err, result) {
                    next(err, result);
                });
        },

        getAllAppKey = function (next){
            var cqlQuery = ["SELECT * FROM ",
                        constants.EVENTS_APIKEY_TBL].join(' ');
        
            client.execute(
                cqlQuery,
                    null,
                    { prepare: true },
                    function (err, result) {
                    next(err, result);
                });
        },
        
        save = function (params, ReadAssetSerial, ctx, next) {
            // Default missing paramaters to 'null'
            var appName = (params.appName || 'null').trim().toLowerCase();
            var eventName = (params.eventName || 'null').trim()
            var eventTime = params.eventTime;
            var userId = (params.userId || 'null').trim();
            var geoLocation = utils._mapToCassandraMap(params.geoLocation || 'null');
            var location = utils._mapToCassandraMap(params.location || 'null');
            var paramsClient = utils._mapToCassandraMap(params.client || 'null');
            var meta = utils._mapToCassandraMap(params.meta || 'null');
            var asset = null;
            var brand = null;
            var pack = null;
            var eventOrigin = ((params.eventOrigin != 'null') ? params.eventOrigin : utils.getDefaultEventOrigin())
            
            if (ReadAssetSerial[1]) {
                serialsCount = ReadAssetSerial[0];
                if (params.assetSerial != null || params.assetSerial != undefined) {
                    splitter = params.assetSerial.split(constants.SERIALS_DELIMITER);
                    switch (serialsCount) {
                        case 1: brand = splitter[0]; break;
                        case 2: brand = splitter[0]; pack = splitter[1]; break;
                        case 3: brand = splitter[0]; pack = splitter[1]; asset = splitter[2]; break;
                        default: break;
                    }
                }
            }
            else {
                var serials = params.serials || 'null';
                serials = JSON.parse(utils._mapToJSON(serials));
                
                if (serials != null) {
                    asset = serials.asset;
                    brand = serials.brand;
                    pack = serials.pack;
                }
            }
            
            if(!utils.isNullorEmpty(asset))
                asset = asset.toString();
            
            if(!utils.isNullorEmpty(brand))
                brand = brand.toString();
            
            if(!utils.isNullorEmpty(pack))
                pack = pack.toString();
            
            //add the client ip if it is not present in the client.ip field
            if (params.client != null || params.client != undefined) {
                var paramsClientCp = JSON.parse(utils._mapToJSON(paramsClient))

                if (paramsClientCp != null) {
                    if (paramsClientCp.ip == undefined || paramsClientCp.ip == null) {
                        paramsClientCp.ip = utils.getIPAddress(ctx);
                        paramsClient = JSON.stringify(paramsClientCp);
                        paramsClient = utils._mapToCassandraMap(paramsClient);
                    }
                }
            }
            else {
                paramsClient = "{\"ip\":\"" + utils.getIPAddress(ctx) + "\"}";
                paramsClient = utils._mapToCassandraMap(paramsClient);
            }
            
            // check for ttl events and if yes then define ttl
            var ttlquery = "";
            var ttl = 0
            if ((ttl = utils.isTTLEvent(eventName)) != 0)
                ttlquery += " USING TTL " + ttl;
            
            validParams = {
                appName: appName,
                eventName: eventName,
                eventTime: eventTime,
                userId: userId ,
                geoLocation: geoLocation,
                location : location ,
                Client : paramsClient ,
                meta : meta,
                asset : asset ,
                brand : brand ,
                pack : pack ,
                eventOrigin : eventOrigin

            };
            
            var jsonString = kinesis.convertToJson(validParams);
            kinesis.writeToKinesis(jsonString, constants.KINESIS_STREAM);

            // Joining an array of strings is more performant than using the '+' operator
            var cqlInsert = [
                "INSERT INTO ",
                constants.EVENTS_EVENTS_TBL,
                "(addedtime, appname, randomnum, shortuuid, assetname, brandname, client, eventname, eventorigin, eventtime, geolocation, location, meta, packname, userid) values (",
                utils.getCurrentTimeStamp(), ",", // addedtime
                "?,", // appname
                "?,", // random number
                "?,", //shortuuid
                "?,", // assetname
                "?,", // brandname
                utils._convertToAllStrings(paramsClient), ",", // client
                "?,", // eventname
                "?,", // eventorigin
                "'", utils.getUTCDate(eventTime), "',", // eventtime
                utils._convertToAllStrings(geoLocation), ",", // geolocation
                utils._convertToAllStrings(location), ",", // location
                utils._convertToAllStrings(meta), ",", // meta
                "?,", // packname
                "?)",  // userid
                ttlquery     // ttl if present  
            ].join('');
            
            client.execute(
                cqlInsert,
          [
                    appName,
                    utils.randomIntInc(constants.RANDOM_LOWER_LIMIT, constants.RANDOM_HIGHER_LIMIT),
                    utils.getUniqueId(),
                    asset,
                    brand,
                    eventName,
                    eventOrigin,
                    pack,
                    userId
                ],
          { prepare : true },
          function (err) {   
                    next(err);
                }  
       
            );
        };
    
    return {
        validateData: validateData,
        save: save,
        retrieveLog: retrieveLog,
        saveErrorLogs: saveErrorLogs,
        saveToEventName: saveToEventName,
        deleteFromEventName: deleteFromEventName,
        getEventName: getEventName,
        updateToEventName: updateToEventName,
        saveAppNameKey: saveAppNameKey,
        deleteAppNameKey: deleteAppNameKey,
        getAppKey: getAppKey,
        getAllAppKey: getAllAppKey,
        updateKey: updateKey,
        getErrorLog: getErrorLog
    };
}();
