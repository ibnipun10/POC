var loopback = require('loopback'),
    cassandra = require('../../server/connectors/cassandra.js'),
    cacheHelper = require('../utils/cacheHelper.js'),
    utils = require('../utils/util.js'),
    apiErrorHandler = require('../utils/apiErrorHandler.js'),
    validateParams = require('../utils/validateParams.js'),
    constants = require('../utils/constants.js');

module.exports = function (Errorlogs) {
    // Disable default remote methods.
    var disableRemoteMethods = (function () {
        var defaultRemoteMethods = ['create', 'upsert', 'find', 'exists', 'existsById', 'findById', 'deleteById', 'prototype.updateAttributes', 'findOne', 'updateAll', 'count'];
        for (var i = 0; i < defaultRemoteMethods.length; i++) Errorlogs.disableRemoteMethod(defaultRemoteMethods[i], true);
    }()),
    
        paramReturns = { type: 'object', root: true },    
        throwError = apiErrorHandler.throwError,

         //validation handler
        keyValidationHandler = function (ctx, unused, next) {
            if (validateParams.validateKey(ctx))
                next();
        };

    Errorlogs.getErrorLogs = function (perPage, callback) {
        try {
            var thisPerPage = perPage || constants.DEFAULT_PERPAGE;
            cassandra.getErrorLog(thisPerPage, function (err, result) {
                callback(err, {
                    result: 'success',
                    response: 'OK',
                    data: {
                        perPage: thisPerPage,
                        events: result.rows
                    }
                })
            })
        } catch (e) {
            var ctx = loopback.getCurrentContext().active.http;
            apiErrorHandler.throwError.internalServerError(ctx, 204, constants.ERROR_EVENTS_API);
        }
    };
    
    Errorlogs.beforeRemote('getErrorLogs', keyValidationHandler);

    Errorlogs.remoteMethod('getErrorLogs', {
        description: 'retreive error logs',
        accepts: [{ arg: 'perPage', type: 'string', required: false}],
        returns: paramReturns,
        http: { path: '/', verb: 'get' }

    });

};
