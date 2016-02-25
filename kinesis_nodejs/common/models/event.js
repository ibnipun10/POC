// TODO: Return proper logType in log request (production/test)

var loopback = require('loopback'),
    cassandra = require('../../server/connectors/cassandra.js'),
    cacheHelper = require('../utils/cacheHelper.js'),
    utils = require('../utils/util.js'),
    apiErrorHandler = require('../utils/apiErrorHandler.js'),
    validateParams = require('../utils/validateParams.js'),
    wait = require('wait.for'),
    constants = require('../utils/constants.js'),
    kinesis = require('../../server/connectors/kinesis.js');
var url = require("url");
var queryString = require('querystring');


module.exports = function (Event) {
    
    // Disable default remote methods.
    var disableRemoteMethods = (function () {
        var defaultRemoteMethods = ['create', 'upsert', 'find', 'exists', 'existsById', 'findById', 'deleteById', 'prototype.updateAttributes', 'findOne', 'updateAll', 'count'];
        for (var i = 0; i < defaultRemoteMethods.length; i++) Event.disableRemoteMethod(defaultRemoteMethods[i], true);
    }()),

        throwError = apiErrorHandler.throwError,

        logValidationHandler = function (ctx, unused, next) {
            
            try {
                var key = ctx.req.get('Authorization');
                params = Object.keys(ctx.req.body).length > 0 ? ctx.req.body : ctx.req.query,

                validateParams.appNameKey(key, params.appName, function validateAppName(bValid) {                                                   // Validate appName and key
                    if (bValid) { next(); return; }
                    throwError.unauthorized(ctx, 301, constants.ERROR_APPNAME);
                    return;
                });
            } catch (e) {
                apiErrorHandler.throwError.internalServerError(ctx, 201, constants.ERROR_EVENTS_API);
            }
        },

       

        throwCustomError = function (errorType, ctx) {
            
            switch (errorType) {

                case constants.VALIDATION_TYPE_SERVER: {
                    apiErrorHandler.throwError.internalServerError(
                        ctx,
                      200,
                      constants.ERROR_EVENTS_API
                    );
                    break;
                }

                case constants.VALIDATION_TYPE_META: {
                    apiErrorHandler.throwError.badRequest(
                        ctx,
                      201,
                      constants.ERROR_META_DATA
                    );
                    break;
                }

                case constants.VALIDATION_TYPE_REQUIREDFIELDS: {
                    apiErrorHandler.throwError.badRequest(
                        ctx,
                      202,
                      constants.ERROR_REQUIRED_FIELDS
                    );
                    break;
                }

                case constants.NULL_REQUIRED_FIELDS: {
                    apiErrorHandler.throwError.badRequest(
                        ctx,
                      202,
                      constants.ERROR_NULL_REQUIRED_FIELDS
                    );
                    break;
                }

            }
        },
    
        validateAppName = function (ctx, eventName, key, params, next, bfurther)
        {
            validateParams.appNameKey(key, params.appName, function validateAppKey(bValid) {
                try {
                    if (bValid) {
                        if(bfurther) { validateData(ctx, eventName, key, params, next); return; }
                        else { next(); }
                    }
                    else { throwError.unauthorized(ctx, 302, constants.ERROR_APPNAME); return; }
                } catch (e) {
                    next(e);
                }
            });
        },
        validateData = function (ctx, eventName, key, params, next) {             
                  
                validateParams.eventName(eventName, function validateEventName(bValid, err) {                                             // Validate event Name
                    try {
                        if (bValid) {
                            validateParams.metaAndRequired(eventName, params, function validateMetaAndSerials(bValid, errtype) {           // Validate meta
                                try {
                                    if (bValid) {
                                        if (!validateParams.eventTime(params.eventTime)) { throwError.badRequest(ctx, 103, constants.ERROR_EVENT_TIME); return; }  // Validate eventTime
                                                    
                                        next(null);
                                    } else {
                                        if (errtype == null)
                                            throwError.badRequest(ctx, 102, constants.ERROR_META_OR_REQUIRED);
                                        else
                                            throwCustomError(errtype, ctx);
                                        return;
                                    }
                                }
                            catch (e) {
                                    next(e);
                                }

                            });
                        } else {
                            if (err)
                                throwError.internalServerError(ctx, 200, constants.ERROR_EVENTS_API);
                            else
                                throwError.badRequest(ctx, 101, constants.ERROR_EVENT_NAME);
                            return;
                                        
                                        
                        }
                    }
                catch (e) {
                        next(e);
                    }
                        
                });                        
              
        },

      // Validation handler
        validationHandler = function (ctx, unused, next) {
            if (utils.validateContentType(ctx)) {
                
                if (!utils.checkEmptyBody(ctx)) { throwError.badRequest(ctx, 102, constants.ERROR_INVALID_DATA); return; }
                
                
                try {
                    var eventName = ctx.args.eventName,
                        params = Object.keys(ctx.req.body).length > 0 ? ctx.req.body : ctx.req.query,
                        key = ctx.req.get('Authorization');
                    
                    var bpost = false;
                    if (Object.keys(ctx.req.body).length > 0)
                        bpost = true;
                    
                    
                    //unable to get '+' in querystring in eventtime
                    //parsing it separately 
                    if (!bpost) {
                        var eventtime = utils.getEventTimeFromurl(ctx);
                        if (eventtime)
                            params.eventTime = eventtime;
                    }
                    validateAppName(ctx, eventName, key, params, next, true);
          
                } catch (e) {
                    apiErrorHandler.throwError.internalServerError(ctx, 202, constants.ERROR_EVENTS_API);
                    return;
                }
            }
        },

        SaveBatchEvents = function (jsonData, key, callback) {
            
            var ctx = loopback.getCurrentContext().active.http;
            var result = "";
            var strRecord = null;
            var strparseRecord = {};
            var events = [];
            strparseRecord = events;
            
            var data = jsonData.events;
            
            for (var i = 0; i < data.length; i++) {
                edata = data[i];
                try {
                    
                    var jsonClient = {};

                    if (jsonData.client != null || jsonData.client != undefined)
                        jsonClient = jsonData.client
                    else
                        jsonClient = edata["client"]

                    validData = {
                        eventName: edata["eventName"],
                        eventTime: edata["eventTime"],
                        appName: jsonData.appName,
                        userId: edata["userId"],
                        geoLocation: jsonData.geoLocation, 
                        location: edata["location"],
                        client: jsonClient,
                        meta: edata["meta"],
                        serials: edata["serials"],
                        eventOrigin: jsonData.eventOrigin,
                        assetSerial: edata["assetSerial"],                       
                        result: undefined
                    };          
                    
                    
                    var result = wait.for(validateData, null, validData.eventName, key, validData)
                    if (!result) {
                        
                        /*This function is used to validate the serial.brands, serials.pack and serials.asset
                         * It is not valdiated in the validation function in beforeRemote as it reuired serials 
                         * values in this function and to save 2 checks we are validating here.
                        */
                        var ReadAssetSerial = [];
                        
                        try {
                            var params = { serials: validData.serials, assetSerial: validData.assetSerial, eventName: validData.eventName.toLowerCase() };
                            ReadAssetSerial = validateParams.validateSerials(params);
                        } catch (e) {
                            validData.result = "error : " + e;
                            strparseRecord.push(validData);
                            continue;                           
                        }


                        // All data should be valid at this point
                        validData.eventOrigin = validData.eventOrigin ? validData.eventOrigin : utils.getDefaultEventOrigin();
                        
                        var res = wait.for(cassandra.save, validData, ReadAssetSerial, ctx)
                        
                        if (!res)
                            validData.result = "Success";
                    
                    }

             
                } catch (e) {                    
                    validData.result = "error : " + e;
                    strparseRecord.push(validData);                
                }
            
                
            }
            
            callback(undefined, {
                response: 'OK',
                data: {
                    logType: utils.getLogType(),
                    events: strparseRecord
                }
            });
        },

        //batchValidationHandler
        batchValidationHandler = function (ctx, unused, next)
        {
            if (utils.validateContentType(ctx)) {
                if (!utils.checkEmptyBody(ctx)) { throwError.badRequest(ctx, 102, constants.ERROR_INVALID_DATA); return; }
                else {
                        //validate appName and key
                   
                            if (!utils.isNullorEmpty(ctx.req.body)) {
                            params = {
                                appName : ctx.req.body.appName
                            }                             
                            var key = ctx.req.get('Authorization');                             
                            validateAppName(ctx, null, key, params, next, false);
                        }
                        else { throwError.badRequest(ctx, 102, constants.ERROR_INVALID_DATA); }                    
                }
                return;
            }
        },

  
      // Accepted parameters
        paramAccepts = [
            { arg: 'eventName', type: 'string', required: true, http: { source: 'path' } },
            { arg: 'eventTime', type: 'string', required: true },
            { arg: 'appName', type: 'string', required: true },
            { arg: 'assetSerial', type: 'string', required: false },
            { arg: 'userId', type: 'string', required: false },
            { arg: 'geoLocation', type: 'string', required: false },
            { arg: 'location', type: 'string', required: false },
            { arg: 'client', type: 'string', required: false },
            { arg: 'eventOrigin', type: 'string', required: false },
            { arg: 'meta', type: 'string', required: false },
            { arg: 'serials', type: 'string', required: false }
        ],

        paramReturns = { type: 'object', root: true };
    
    // Loopback Remote Method 'before' hook (validation)
    Event.beforeRemote('saveEvent', validationHandler);
    Event.beforeRemote('getLog', logValidationHandler);
    Event.beforeRemote('batchSaveEvents', batchValidationHandler);
    
    // Save Event (Loopback Remote Method)
    Event.saveEvent = function (eventName, eventTime, appName, assetSerial, userId, geoLocation, location, client, eventOrigin, meta, serials, callback) {
        var ctx = loopback.getCurrentContext().active.http;
        try {
            
            
            /*This function is used to validate the serial.brands, serials.pack and serials.asset
             * It is not valdiated in the validation function in beforeRemote as it reuired serials 
             * values in this function and to save 2 checks we are validating here.
            */            
            var ReadAssetSerial = [];

            try {
                var params = { serials: serials, assetSerial: assetSerial, eventName: eventName.toLowerCase() };
                ReadAssetSerial = validateParams.validateSerials(params);
            } catch (e) {
                throwError.badRequest(ctx, 102, constants.ERROR_SERIALS);
                return;
            }


            var bpost = false;
            if (Object.keys(ctx.req.body).length > 0)
                bpost = true;
            
            if (!bpost)
                eventTime = utils.getEventTimeFromurl(ctx);
            // All data should be valid at this point
            var eventOrigin = eventOrigin ? eventOrigin : utils.getDefaultEventOrigin(),
                
                addedTime = utils.getCurrentTimeStamp(),
          
                validData = {
                    eventName: eventName,
                    eventTime: eventTime,
                    appName: appName,
                    userId: userId,
                    geoLocation: geoLocation,
                    location: location,
                    client: client,
                    meta: meta,
                    serials: serials,
                    eventOrigin: eventOrigin,
                    addedTime: addedTime,
                    assetSerial: assetSerial
                },

                successResponse = { result: 'success', response: 'OK' };
            
            cassandra.save(validData, ReadAssetSerial, ctx, function (err) {
                
                if (err)
                    throwError.internalServerError(ctx, 203, constants.ERROR_EVENTS_API);
                else
                    callback(err, successResponse);
                return;
            });
        } catch (e) {            
            apiErrorHandler.throwError.internalServerError(ctx, 203, constants.ERROR_EVENTS_API);
        }
    };
    
    // Save Event (Loopback Remote Method)
    Event.getLog = function (appName, perPage, callback) {
        try {
	  
	    
            var thisPerPage = Number(perPage);
            if(isNaN(perPage) || thisPerPage <= 0 || thisPerPage > 1000)
	    	    thisPerPage = constants.DEFAULT_PERPAGE;
	        else
		        thisPerPage = Math.ceil(perPage);
            
            
            var validData = {
                    appName: appName,
                    perPage: thisPerPage
                };
            
            cassandra.retrieveLog(validData, function (err, result) {
                //if successful, result will have array of rows
                
                if (result == undefined)
                    result = { rows: 0 }
                
                if (err)
                    apiErrorHandler.throwError.internalServerError(ctx, 204, constants.ERROR_EVENTS_API);
                else
                callback(null, {
                    result: 'success',
                    response: 'OK',
                    data: {
                        logType: utils.getLogType(),
                        perPage: thisPerPage,
                        events: result.rows
                    }
                });
            });
        } catch (e) {
            var ctx = loopback.getCurrentContext().active.http;
            apiErrorHandler.throwError.internalServerError(ctx, 204, constants.ERROR_EVENTS_API);
        }
    };
    
    Event.batchSaveEvents = function (req, callback) {
        
        var ctx = loopback.getCurrentContext().active.http;
        
      
        try {
            var jsonData = req.body;
        } catch (e) {
            throwError.badRequest(ctx, 102, constants.ERROR_INVALID_DATA);
            return;
        }
        
        
        
        var key = req.get('Authorization');
        
        
        
        //even if we are able to insert 1 single data, it should be success
        var bsuccess = false;
        
        if (jsonData.events === undefined) {
            throwError.badRequest(ctx, 102, constants.ERROR_NO_DATA);
            return;
        }
        
        wait.launchFiber(SaveBatchEvents, jsonData, key, callback);
            
       

        //callback(undefined, successResponse);
    };
    
    // Register Log GET method on Loopback model
    Event.remoteMethod(
        'getLog',
    {
            description: 'Retrieve event log',
            accepts: [
                { arg: 'appName', type: 'string', required: true },
                { arg: 'perPage', type: 'string', required: false }
            ],
            returns: paramReturns,
            http: { path: '/log', verb: 'get' }
        }
    );
    
    // Register Post method on Loopback model to buld upload
    Event.remoteMethod(
        'batchSaveEvents',
        {
            description: 'Bulk upload events',
            accepts: [
                {
                    arg: 'req', type: 'object', http: { source: 'req' }
                }
            ],
            returns: paramReturns,
            http: { path: '/batch', verb: 'post' }
        }
    )
    
    // Register GET method on Loopback model
    Event.remoteMethod(
        'saveEvent',
    {
            description: 'Save event via GET request',
            accepts: paramAccepts,
            returns: paramReturns,
            http: { path: '/:eventName', verb: 'get' }
        }
    );
    
    // Register POST method on Loopback model
    Event.remoteMethod(
        'saveEvent',
    {
            description: 'Save event via POST request',
            accepts: paramAccepts,
            returns: paramReturns,
            http: { path: '/:eventName', verb: 'post' }
        }
    );
};
