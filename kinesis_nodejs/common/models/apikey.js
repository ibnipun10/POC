var loopback = require('loopback'),
    cassandra = require('../../server/connectors/cassandra.js'),
    cacheHelper = require('../utils/cacheHelper.js'),
    utils = require('../utils/util.js'),
    apiErrorHandler = require('../utils/apiErrorHandler.js'),
    validateParams = require('../utils/validateParams.js'),
    constants = require('../utils/constants.js');

module.exports = function (Apikey) {
    // Disable default remote methods.
    var disableRemoteMethods = (function () {
        var defaultRemoteMethods = ['create', 'upsert', 'find', 'exists', 'existsById', 'findById', 'deleteById', 'prototype.updateAttributes', 'findOne', 'updateAll', 'count'];
        for (var i = 0; i < defaultRemoteMethods.length; i++) Apikey.disableRemoteMethod(defaultRemoteMethods[i], true);
    }()),
    
    paramReturns = { type: 'object', root: true },    
    throwError = apiErrorHandler.throwError,

    // Validation handler
    addValidationHandler = function (ctx, unused, next) {
            if (utils.validateContentType(ctx)) {
                if (validateParams.validateKey(ctx)) {
                    try {
                        
                        var appName = ctx.args.appname;
                        var key = ctx.req.body.key;
                        
                        if (utils.isNullorEmpty(key)) {
                            throwError.badRequest(ctx, 101, constants.ERROR_INVALID_DATA);
                            return;
                        }
                        // validate appname
                        validateParams.appName(appName, function (bValid) {
                            if (!bValid) next();
                            else { throwError.badRequest(ctx, 101, constants.ERROR_APPNAME_ALREADY_PRESENT); return; }
                        });
                    } catch (e) {
                        throwError.internalServerError(ctx, 202, constants.ERROR_EVENTS_API);
                    }
                }
            }
    },
    
    //validation handler for delete
    keyValidationHandler = function (ctx, unused, next) {
        if (validateParams.validateKey(ctx))
            next();
    },
    
    ValidationHandler = function (ctx, unused, next){
            if (validateParams.validateKey(ctx)) {
                try {
                    
                    var appName = ctx.args.appname;
                    
                    // validate appname
                    validateParams.appName(appName, function (bValid) {
                        if (bValid) {
                            next();
                            return;
                        }
                        else { throwError.badRequest(ctx, 101, constants.ERROR_APPNAME_NOT_PRESENT); }
                    });
                } catch (e) {
                    throwError.internalServerError(ctx, 202, constants.ERROR_EVENTS_API);
                }
            }
    },

    UpdateValidationHandler = function (ctx, unused, next){
        if (utils.validateContentType(ctx)) {
            if (validateParams.validateKey(ctx)) {
                try {
                        
                    var appName = ctx.args.appname;
                    var key = ctx.req.body.key;
                        
                    if (utils.isNullorEmpty(key)) {
                        throwError.badRequest(ctx, 101, constants.ERROR_INVALID_DATA);
                        return;
                    }
                    // validate appname
                    validateParams.appName(appName, function (bValid) {
                        if (bValid) next();
                        else { throwError.badRequest(ctx, 101, constants.ERROR_APPNAME_NOT_PRESENT); return; }
                    });
                } catch (e) {
                    throwError.internalServerError(ctx, 202, constants.ERROR_EVENTS_API);
                }
            }
        }
    }

    
    // Loopback Remote Method 'before' hook (validation)
    Apikey.beforeRemote('saveAppKey', addValidationHandler);
    Apikey.beforeRemote('deleteAppKey', ValidationHandler);
    Apikey.beforeRemote('getAppKey', ValidationHandler);
    Apikey.beforeRemote('getAllAppKey', keyValidationHandler);
    Apikey.beforeRemote('updateKey', UpdateValidationHandler);

    //Update key for a given appname, if it exists
    Apikey.updateKey = function (appname, key, callback) {
        
        var ctx = loopback.getCurrentContext().active.http;
        successResponse = { result: 'success', response: 'OK' };
        var newKey = key;
        cassandra.updateKey(appname, newKey, function (err) {
            if (err)
                throwError.badRequest(ctx, 102, constants.ERROR_INVALID_DATA);
            else
                callback(err, successResponse);
            return;
        }); 
    };
    

    Apikey.getAppKey = function (appName, callback) {
        cassandra.getAppKey(appName, function (err, result) {
            //if successful, result will have array of rows
            callback(err, {
                result: 'success',
                response: 'OK',
                data: {
                    logType: utils.getLogType(),
                    eventnames: result.rows
                }
            });
        });
    };
    
    Apikey.getAllAppKey = function (callback) {
        cassandra.getAllAppKey(function (err, result) {
            //if successful, result will have array of rows
            callback(err, {
                result: 'success',
                response: 'OK',
                data: {
                    logType: utils.getLogType(),
                    eventnames: result.rows
                }
            });
        });
    };
    
    Apikey.saveAppKey = function (appName, key, callback) {
        
        var ctx = loopback.getCurrentContext().active.http;
        successResponse = { result: 'success', response: 'OK' };
        
        cassandra.saveAppNameKey(appName, key, function (err) {
            if (err)
                throwError.badRequest(ctx, 102, constants.ERROR_INVALID_DATA);
            else
                callback(err, successResponse);
            return;
        });
    };
    
    Apikey.deleteAppKey = function (appName, callback){
        
        var ctx = loopback.getCurrentContext().active.http;
        successResponse = { result: 'success', response: 'OK' };
        cassandra.deleteAppNameKey(appName, function (err) {
            if (err)
                throwError.badRequest(ctx, 102, constants.ERROR_INVALID_DATA);
            else
                callback(err, successResponse);
            return;
        });
    }

    // Register POST method on Loopback model
    Apikey.remoteMethod(
        'saveAppKey',
    {
            description: 'Save app key via POST request',
            accepts: [
                { arg: 'appname', type: 'string', required: true, http: { source: 'path' } },
                { arg: 'key', type: 'string', required: true}
            ],
            returns: paramReturns,
            http: { path: '/:appname', verb: 'post' }
    });
    
    // Register Delete method on Loopback model
    Apikey.remoteMethod(
        'deleteAppKey',
    {
            description: 'delete AppKey via DEL request',
            accepts: [
                { arg: 'appname', type: 'string', required: true, http: { source: 'path' } }
            ],
            returns: paramReturns,
            http: { path: '/:appname', verb: 'del' }
    });
    
    // Register get method on Loopback model to get app and key
    Apikey.remoteMethod(
        'getAppKey',
    {
            description: 'get app name and appkey via get request',
            accepts: [
                { arg: 'appname', type: 'string', required: true, http: { source: 'path' } }
            ],
            returns: paramReturns,
            http: { path: '/:appname', verb: 'get' }
    });
    
    // Register get method on Loopback model to get all event
    Apikey.remoteMethod(
        'getAllAppKey',
    {
            description: 'get all appkey via get request',
            returns: paramReturns,
            http: { path: '/', verb: 'get' }
        });

    Apikey.remoteMethod(
        'updateKey', {
            description: 'update key for a given appname',
            accepts: [
                        { arg: 'appname', type: 'string', required: true, http: { source: 'path' }},
                        { arg: 'key', type: 'string', required: true}
                    ],
            returns: paramReturns,
            http: { path: '/:appname', verb: 'put' }
        }
    );
};
