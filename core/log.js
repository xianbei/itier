// (C) 2011-2012 Alibaba Group Holding Limited.
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU General Public License 
// version 2 as published by the Free Software Foundation. 

// Authors :fengyin <fengyin.zym@taobao.com>

var fs = require('fs');
var dateFormat = require('./date_format.js');
var conf = require('../conf');

var FILE_SPLIT_INTERVAL = 'DAY';//1d
var BUFFER_CHECK_INTERVAL = 2000;//2s
var BUFFER_FLUSH_LEN = 512;
var LOG_DIRECTORY = conf.logDirectory ;

var fileMap = {
  'default' : {
    pathPrefix            : 'iframe.log.'//,
  },
  'access' : {
    pathPrefix            : 'access.log.'
  },
  'slow' : {
    pathPrefix            : 'slow.log.'
  },
  'error' : {
    pathPrefix            : 'error.log.'
  },
  'excp' : {
    pathPrefix            : 'excp.log.'
  }
};

//initial setting
var curDay = (new Date()).getDate();

function genFilePostfix(){
  var dnow = new Date();
  return dnow.format('yyyy-mm-dd');
}

function genTimeStamp(){
  var dnow = new Date();
  return dnow.format('HH:MM:ss');
}

function debug(str){
  console.log(str);
}

function inspect(obj){
  console.log(require('util').inspect(obj, false, 10));
}

function LogFile(options){
  this.pathPrefix = options.pathPrefix;

  this.buffers = [];
  this.bufferCheckInterval = options.bufferCheckInterval || BUFFER_CHECK_INTERVAL;

  this.init();
}

LogFile.prototype.push = function(str){
  this.buffers.push(str);
  if(this.buffers.length >= BUFFER_FLUSH_LEN){
    this._flush();
  }
} 

LogFile.prototype._flush = function(){
  if(this.buffers.length > 0 && this.stream){
    this.buffers.push('');
    var str = this.buffers.join('\n');
    this.stream.write(str);
    //debug('buffers length: ' + this.buffers.length); 
    this.buffers = [];
  }
}

LogFile.prototype.destroy = function(){
  this._flush();

  if(this.bufferCheckTimer){
    clearInterval(this.bufferCheckTimer);
    this.bufferCheckTimer = null;
  }
  if(this.stream){
    this.stream.end();
    this.stream.destroySoon();
    this.stream = null;
  }
}


LogFile.prototype.init = function(){
  //debug('log init ' + this.pathPrefix);
  var self = this;
  var path = LOG_DIRECTORY + this.pathPrefix + genFilePostfix(); 
  //debug(path);
  //inspect(conf);
  this.stream = fs.createWriteStream(path, {flags : 'a'});
  
  this.bufferCheckTimer = setInterval(function(){
      self._flush(); 
  }, this.bufferCheckInterval);
}

LogFile.prototype.restart = function(){
  this.destroy();
  this.init();
}

var logMap = {};

//exports.init = function(){
for(var id in fileMap){
  logMap[id] = new LogFile(fileMap[id]);
}
//}

function push2File(str, id){
  var logFile = logMap[id];
  
  var dnow = new Date();
  if(dnow.getDate() != curDay){
  //if(dnow.getMinutes() != curMinute){
    logFile.restart();
    curDay = dnow.getDate();
    //curMinute = dnow.getMinutes();
  }
  logFile.push(dnow.format('HH:MM:ss') + '\t' + str);
}

exports.info = function(str){
  push2File(str, 'default');
}

exports.access = function(str){
  push2File(str, 'access');
}

exports.slow = function(str){
  push2File(str, 'slow');
}

exports.error = function(str){
  push2File(str, 'error');
}

exports.excp = function(str){
  push2File(str, 'excp');
}
