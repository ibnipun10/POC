var cass = require('../../server/connectors/cassandra.js')

module.exports = function () {

  // Form eror response
  var _errorResponse = function (name, code, msg) {
    return {
      result:   'failure',
      response: name,
      error:    { code: code, message: msg }
    }
    },

    writeErrorLogsToCassandra = function (code, errorMsg, reason)
    {
        try {
                //write the error to the db               
            cass.saveErrorLogs(code, errorMsg, reason);
        }
        catch(ex) { }
    },

  // Throw an http error back to the client
  throwError = {
            badRequest: function (ctx, code, errorMsg) {
                
                writeErrorLogsToCassandra(code, 'Bad Request', errorMsg);
                
                if (ctx)
                    ctx.res.status(400).send(_errorResponse('Bad Request', code, errorMsg));
                else
                    throw errorMsg; },

            unauthorized: function (ctx, code, errorMsg) {
                writeErrorLogsToCassandra(code, 'Unauthorized', errorMsg);
               
                if (ctx)
                    ctx.res.status(401).send(_errorResponse('Unauthorized', code, errorMsg));
                else
                    throw errorMsg;},

            internalServerError: function (ctx, code, errorMsg) {
                
                writeErrorLogsToCassandra(code, 'Internal Server Error', errorMsg);
                
                if (ctx)
                    ctx.res.status(500).send(_errorResponse('Internal Server Error', code, errorMsg));
                else
                    throw errorMsg;}
  };

  return {
    throwError: throwError
  };

}();