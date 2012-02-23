// (C) 2011-2012 Alibaba Group Holding Limited.
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU General Public License 
// version 2 as published by the Free Software Foundation. 

// Authors :fengyin <fengyin.zym@taobao.com>

require('./env');
var cp = require('child_process');
var TCP = process.binding('tcp_wrap').TCP;
var childMng = require("./child_mng");
var conf = require('../conf');

//user settings
//var ADDRESS = '0.0.0.0';
var PORT = conf.port;
var WORKER_NUMBER = conf.workerNumber;

var GRACE_EXIT_TIME = 2000;//2s
var WORKER_PATH = __dirname + '/tcpWorker.js';
var WORKER_HEART_BEAT = 10*1000;//10s, update memory ,etc

function startWorker(){
  for(var i = 0; i < WORKER_NUMBER; i++){
    var c  =  cp.fork(WORKER_PATH);
    childMng.push(c);
  }
  output('start workers: ' + WORKER_NUMBER);
/*
  setInterval(function(){
    inspect(childMng.getStatus());
    childMng.updateStatus();
  },WORKER_HEART_BEAT);
  */
}

var server = null;
var exitTimer = null;
function aboutExit(){
  if(exitTimer) return;

  server.close();
  childMng.kill();

  exitTimer = setTimeout(function(){
    output('master exit...');

    //log.destroy();
    process.exit(0);
  }, GRACE_EXIT_TIME);
}

//var ADDRESS = '127.0.0.1';
var ADDRESS = '0.0.0.0';
var BACK_LOG = 128;

function onconnection(handle){
  //output('master on connection');
  childMng.disHandle(handle);
  handle.close();
}

function startServer(){
  output('will bind port');
  server = new TCP();

  server.bind(ADDRESS, PORT);
  server.onconnection = onconnection;
  server.listen(BACK_LOG);
  output('bind ok');
}

void main(function(){
  
  startServer();
  startWorker();

  output('master is running...');
  process.on('SIGINT'  , aboutExit);
  process.on('SIGTERM' , aboutExit);

});
