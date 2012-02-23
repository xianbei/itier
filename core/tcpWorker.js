// (C) 2011-2012 Alibaba Group Holding Limited.
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU General Public License 
// version 2 as published by the Free Software Foundation. 

// Authors :fengyin <fengyin.zym@taobao.com>

require('./env');
var dispatch = require('../src/dispatch').fn;
var net = require('net');
var http = require('http');

var GRACE_EXIT_TIME = 1500;

var server = null;
var exitTimer = null;
var childReqCount = 0;

function aboutExit(){
  if(exitTimer) return;

  //server.close();
  exitTimer = setTimeout(function(){
    output('worker will exit...');
    output('child req total : ' + childReqCount);

    process.exit(0);
  },GRACE_EXIT_TIME);
}

function onhandle(self, handle){
  if(self.maxConnections && self.connections >= self.maxConnections){
    handle.close();
    return;
  }
  var socket = new net.Socket({
    handle : handle,
    allowHalfOpen : self.allowHalfOpen 
  });
  socket.readable = socket.writable = true;
  socket.resume();
  self.connections++;
  socket.server = self;
  self.emit('connection', socket);
  socket.emit('connect');
  //output('get a connection');
}

function processInit(){
  output('worker init now...');
  process.on("SIGINT"  ,aboutExit);
  process.on("SIGTERM" ,aboutExit);

  var conf = require('../conf');
  var loadCfg = require("../src/hotcfg").Cfg3;
  loadCfg(conf.resourcesDirectory, 'ActionLoader');

  //init memcache socket pool
  var MCfg = conf.mcskin;
  if(MCfg){
    global.MCSkin = require("mcskin");
    global.MCSkin.init(MCfg.host, MCfg.port, MCfg.maxSockets);
  }
  if(conf.run_mode == 'release'){
    global.log = require('./log');
  }

  process.on('message',function(m ,handle){
    if(handle){
      onhandle(server, handle);
    }
    if(m.status == 'update'){
      process.send({'status' : process.memoryUsage()});
    }
  });
}

void main(function(){

  processInit();

  server = http.createServer(function(req, res){
	  var dis = dispatch(req, res);
	  dis.load();
    childReqCount++;
  });


  output('worker is running...');
});
