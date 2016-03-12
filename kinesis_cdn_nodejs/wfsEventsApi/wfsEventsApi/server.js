var constants = require('./constants.js'),
    Hapi = require('hapi'),
    fs = require('fs'),
    utils = require('./utils.js'),
    kinesis = require('./kinesis.js');

// Create a server with a host and port
var server = new Hapi.Server();

server.connection({
    host: constants.HOST,
    port: constants.PORT
});

server.route({
    method: 'GET',
    path: '/post',
    handler: function (request, reply) {
        
        try {
            console.log(request.raw.req.headers);
            var headers = request.raw.req.headers;
            
            var uri = headers["x-ec-uri"];
            var projectid = headers["x-ec-projectid"];
            var ip = headers["x-forwarded-for"];
            var timestamp = headers["x-ec-timestamp"];
            var useragent = headers["x-ec-user-agent"];
            var referrer = headers["x-ec-referer"];
            var httpstatus = headers["x-ec-returnstatus"];
            
            if (useragent != null && useragent != undefined)
                useragent = '\"' + useragent + '\"';
            
            var params = [uri, projectid, ip, timestamp, useragent, referrer, httpstatus];
            var paramsCsv = utils.convertTocsv(params);
            kinesis.writeToKinesis(paramsCsv)
            
            reply({'reply':'sucess'}, 200);
        }
        catch (ex) {
            reply({'reply':'failiure'}, 500);
        }
         
    }
});

// Start the server
server.start((err) => {

if (err) {
    throw err;
}
console.log('Server running at:', server.info.uri);
});
