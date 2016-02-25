var loopback = require('loopback'),
    cassandra = require('../../server/connectors/cassandra.js'),
    cacheHelper = require('../utils/cacheHelper.js'),
    utils = require('../utils/util.js'),
    apiErrorHandler = require('../utils/apiErrorHandler.js'),
    validateParams = require('../utils/validateParams.js'),
    constants = require('../utils/constants.js');



module.exports = function (EventName) {
    // Disable default remote methods.
    var disableRemoteMethods = (function () {
        var defaultRemoteMethods = ['create', 'upsert', 'find', 'exists', 'existsById', 'findById', 'deleteById', 'prototype.updateAttributes', 'findOne', 'updateAll', 'count'];
        for (var i = 0; i < defaultRemoteMethods.length; i++) EventName.disableRemoteMethod(defaultRemoteMethods[i], true);
    }()),

    // Accepted parameters
    paramAccepts = [
        { arg: 'eventName', type: 'string', required: true, http: { source: 'path' } },
        { arg: 'meta', type: 'string' },
        { arg: 'requiredfields', type: 'string' },
        { arg: 'ttl', type: 'number'}],
    
    paramReturns = { type: 'object', root: true };
        
    throwError = apiErrorHandler.throwError,

    validateData = function (ctx, params) {
        
        try {
            
            if (validateParams.checkValidFormat(params.meta) && 
                    validateParams.checkValidFormat(params.requiredfields)) {
                return true;
            } else {
                throwError.badRequest(ctx, 102, constants.ERROR_INVALID_DATA);
                    
            }
                   
        } catch (e) {
            throwError.internalServerError(ctx, 201, constants.ERROR_EVENTS_API);
        }
        
        return false;
       
    },

    //validation handler for delete
    keyValidationHandler = function (ctx, unused, next) {
        if (validateParams.validateKey(ctx))
            next();
    },

    // Validation handler
    addValidationHandler = function (ctx, unused, next) {
        if (utils.validateContentType(ctx)) {
            try {
                
                validateParams.validateKey(ctx);
                var eventName = ctx.args.eventName,
                    params = Object.keys(ctx.req.body).length > 0 ? ctx.req.body : ctx.req.query;
                
                // validate eventname
                validateParams.eventName(eventName, function (bValid) {
                    if (!bValid) {
                        if (validateData(ctx, params))
                            next();
                    }
                    else { throwError.badRequest(ctx, 101, constants.ERROR_EVENTALREADY_PRESENT); }
                });
            } catch (e) {
                throwError.internalServerError(ctx, 202, constants.ERROR_EVENTS_API);
            }
        }
    },
    
    //Validation handler for update events
    ValidationHandler = function (ctx, unused, next) {
        try {
            
            validateParams.validateKey(ctx);
            var eventName = ctx.args.eventName,
                params = Object.keys(ctx.req.body).length > 0 ? ctx.req.body : ctx.req.query;
            
            // validate eventname
            validateParams.eventName(eventName, function (bValid) {
                if (bValid) {
                    if (validateData(ctx, params))
                        next();
                }
                else
                    throwError.badRequest(ctx, 101, constants.ERROR_EVENT_NAME);
            });
          
        } catch (e) {
            throwError.internalServerError(ctx, 202, constants.ERROR_EVENTS_API);
        }
    };
    
    // Loopback Remote Method 'before' hook (validation)
    EventName.beforeRemote('saveEventName', addValidationHandler);
    EventName.beforeRemote('deleteEventName', ValidationHandler);
    EventName.beforeRemote('getEventName', ValidationHandler);
    EventName.beforeRemote('updateEventName', ValidationHandler);

    EventName.saveEventName = function (eventName, meta, requiredfields, ttl, callback)
    {
        var validData = {
            eventName: eventName,           
            meta: meta,
            requiredfields: requiredfields,
            ttl: ttl        
        };
        
        successResponse = { result: 'success', response: 'OK' };
        var ctx = loopback.getCurrentContext().active.http;

        cassandra.saveToEventName(validData, function (err) {            
            if (err)
                throwError.badRequest(ctx, 102, constants.ERROR_INVALID_DATA);
            else
                callback(err, successResponse);
            return;
        });
    };
    
    EventName.updateEventName = function (eventName, meta, requiredfields, ttl, callback) {
        var validData = {
            eventName: eventName,           
            meta: meta,
            requiredfields: requiredfields,
            ttl: ttl
        };
        
        successResponse = { result: 'success', response: 'OK' };
        var ctx = loopback.getCurrentContext().active.http;
        
        cassandra.updateToEventName(validData, function (err) {
            if (err)
                throwError.badRequest(ctx, 102, constants.ERROR_INVALID_DATA);
            else
                callback(err, successResponse);
            return;
        });
    };
    
    EventName.deleteEventName = function (eventName, callback) {
        successResponse = { result: 'success', response: 'OK' };
        var ctx = loopback.getCurrentContext().active.http;

        cassandra.deleteFromEventName(eventName, function (err) {
            if (err)
                throwError.badRequest(ctx, 102, constants.ERROR_INVALID_DATA);
            else
                callback(err, successResponse);
            return;
        });

    };
    
    EventName.getEventName = function (eventName, callback) {        
        cassandra.getEventName(eventName, function (err, result) {
            //if successful, result will have array of rows
            callback(err, {
                result: 'success',
                response: 'OK',
                data: {
                    logType: 'production',
                    eventnames: result.rows
                }
            });
        });
    };

    // Register POST method on Loopback model
    EventName.remoteMethod(
        'saveEventName',
    {
        description: 'Save eventName via POST request',
        accepts: paramAccepts,
        returns: paramReturns,
        http: { path: '/:eventName', verb: 'post' }
    });

    // Register Delete method on Loopback model
    EventName.remoteMethod(
        'deleteEventName',
    {
        description: 'delete eventName via POST request',
        accepts: [
            { arg: 'eventName', type: 'string', required: true, http: { source: 'path' } }
        ],
        returns: paramReturns,
        http: { path: '/:eventName', verb: 'del' }
    });

    // Register get method on Loopback model to get event
    EventName.remoteMethod(
        'getEventName',
    {
        description: 'get eventName via get request',
        accepts: [
            { arg: 'eventName', type: 'string', required: true, http: { source: 'path' } }
        ],
        returns: paramReturns,
        http: { path: '/:eventName', verb: 'get' }
     });

     // Register put method on Loopback model to update event
    EventName.remoteMethod(
        'updateEventName',
    {
        description: 'get eventName via get request',
        accepts: paramAccepts,
        returns: paramReturns,
        http: { path: '/:eventName', verb: 'put' }
    });
};
