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
    path: '/',
    handler: function (request, reply) {
        
        try {          
            reply({ 'reply': 'sucess' }, 200);
        }
        catch (ex) {
            reply({ 'reply': 'failiure' }, 500);
        }
         
    }
});

server.route({
    method: 'GET',
    path: '/post',
    handler: function (request, reply) {
        
        try {
            var headers = request.raw.req.headers;
            
            var uri = headers["X-EC-Uri"];
            var projectid = headers["X-EC-projectid"];
            var ip = headers["X-EC-IP"];
            var timestamp = headers["X-EC-Timestamp"];
            var useragent = headers["X-EC-User-Agent"];
            var referrer = headers["X-EC-Referer"];
            var httpstatus = headers["X-EC-ReturnStatus"];

            
            if (useragent != null && useragent != undefined)
                useragent = useragent.replace(',','');
            
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
