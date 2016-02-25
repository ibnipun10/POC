 var path = require('path'),
	 fs = require("fs");

exports.privateKey = fs.readFileSync('/home/ubuntu/SamsEventsApi/ssl/sasandbox.swyftmedia.com.key').toString();
exports.certificate = fs.readFileSync('/home/ubuntu/SamsEventsApi/ssl/sasandbox.swyftmedia.com.crt').toString();
exports.caCertificate = fs.readFileSync('/home/ubuntu/SamsEventsApi/ssl/DigiCertCA.crt').toString();
