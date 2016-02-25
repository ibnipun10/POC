var moment = require('moment');
var os = require("os");
var constants = require("./constants.js");
var sha1 = require('sha1');
var uuid = require('node-uuid');
var shortid = require('shortid');
var cacheHelper = require('./cacheHelper.js');

module.exports = {
    randomIntInc: function (low, high) {
        return Math.floor(Math.random() * (high - low + 1) + low);
    },
    
    getListOfNumbers: function (){
        
        var list = "("
        for (var i = constants.RANDOM_LOWER_LIMIT; i <= constants.RANDOM_HIGHER_LIMIT; i++) {
            list += i;
            if (i < constants.RANDOM_HIGHER_LIMIT)
                list += ",";
        }

        list += ")";

        return list;
    },

    getCurrentTimeStamp: function() {
        return new Date().getTime();
    },
    
    isNumber: function isNumeric(n) {
        return isFinite(n);
    },
    
    getUTCDate: function (date) {
        return moment(date).utc().format("YYYY-MM-DD HH:mm:ssZ"); 
        
    },

    convertDateToCassandra: function (date) {
        
        var newDate = module.exports.getUTCDate(date);
        return moment(newDate).valueOf();
    },

  getTimeZone: function(date) {
        return parseFloat(moment.parseZone(date).utcOffset() / 60);
  },

  getCurrentDate: function() {
    return new Date(module.exports.getCurrentTimeStamp);
  },

  getDefaultEventOrigin: function() {
    return 'API';
    },

    getIPAddress: function (ctx){
        return ctx.req.ip;
    },

    getUserAgent: function (ctx){
        return ctx.req.get('user-agent');
    },

    getHostName: function (ctx){
        return os.hostname(); 
    },
    
    getUniqueId: function (){
        return shortid.generate();
    },
    
    checkContentType: function (ctx) {
        if (ctx.req.headers["content-type"].indexOf("application/json") > -1)
            return true;
        else
            return false;        
     },
    
    checkEmptyBody : function (ctx)
    {
        if (ctx.req.method === "POST") {
            var count = 0;
            count = Object.keys(ctx.req.body).length;
            if (count == 0) return false;
        }
        return true;
    },
    
    //Return true if the logtype is Production else false.
    getLogType : function (){
        if (constants.IS_PRODUCTION)
            return 'production';
        else
            return 'sandbox';
    },
    
    //
    isNullorEmpty: function (param) {
        
        if (param != undefined && param != null) {
            if (typeof param === 'string' && !param.trim())
                return true;
            else
                return false;
        }
        else
            return true;
    },


    //If query string has a '+' then it will not take '+' sign in eventtime
    //need to parse it from the request packet
    getEventTimeFromurl: function(ctx){
    
        var decodeURI = decodeURIComponent(ctx.req.url);
        var eventTime = undefined;
        const strEventTime = "eventTime=";
        
        var eventTimeindex = decodeURI.search(strEventTime);
        if (eventTimeindex == -1){ return null; }
        
        var aftereventtime = decodeURI.substring(eventTimeindex + strEventTime.length);
        var endIndex = aftereventtime.indexOf('&');
        if (endIndex == -1)
            eventTime = aftereventtime;
        else
            eventTime = aftereventtime.substring(0, endIndex);

        return eventTime;
                
    },

    _mapToJSON: function (map) {
        if (map != null && map != undefined)
            return map.replace(/'/g, '"');
        else
            return null;
    },
    _mapToCassandraMap: function (json) {
        if (json != null && json != undefined)
            return json.replace(/"/g, "'");
        else
            return null;
    },

    _convertToAllStrings: function (meta) {
        if (meta != null && meta != undefined) {
            //var json = module.exports._mapToJSON(meta);
            json = meta.replace(/:(\s*)(\d+)(\.?)(\d*)([,\}\s])/g, ":'$2$3$4'$5");
            
            return json;
        }
        else
            return null;
    },

    //Validates content type if POST request is made
    validateContentType : function (ctx)
    {    
        if (ctx.req.method === 'POST' || ctx.req.method == 'PUT') {
            if (!(module.exports.checkContentType(ctx))) {
                throwError.badRequest(ctx, 102, constants.ERROR_BAD_REQUEST);
                return false;
            }
        }
        return true;
    },

    getSha1 : function (msg){
        return sha1(msg);
    },

    getkeyfromSha1 : function (msg){
        var sha1 = module.exports.getSha1(msg);
        return sha1.substring(0, 24);
    },

    getRandomKey : function (){
        return module.exports.getkeyfromSha1(uuid.v1());
    },

    isTTLEvent : function (eventname){
        if (cacheHelper.isKeyPresent(eventname, constants.CACHETTL)) {
            var ttl = cacheHelper.getValue(eventname, constants.CACHETTL);
            
            if (ttl == null || ttl == undefined)
                return 0;
            else
                return ttl
        }
        else
            return 0;
    }
};