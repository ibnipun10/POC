var loopback = require('loopback');
var boot = require('loopback-boot');
var https = require('https');
var http = require('http');
var constants = require('../common/utils/constants.js');
var app = module.exports = loopback();

app.start = function () {
    var server;
    var protocol = "https";
    if (!constants.ENABLE_HTTPS) {
        var server = http.createServer(app);
        protocol = "http";
    }
    else {
        var sslConfig = require('./ssl-config.js');
        var options = {
            key: sslConfig.privateKey,
            cert: sslConfig.certificate,
            ca:[
             sslConfig.caCertificate
            ]
        };
        var server = https.createServer(options, app);
    }
    
    server.listen(app.get('port'), function () {        
        var baseUrl = protocol + '://' + app.get('host') + ':' + app.get('port');
            app.enable('trust proxy')
    		app.emit('started', baseUrl);
    		console.log('LoopBack server listening @ %s%s', baseUrl, '/');
  		});
  	return server;
};

// Bootstrap the application, configure models, datasources and middleware.
// Sub-apps like REST API are mounted via boot scripts.
boot(app, __dirname, function(err) {
  if (err) throw err;

  // start the server if `$ node server.js`
  if (require.main === module)
    app.start();
});
