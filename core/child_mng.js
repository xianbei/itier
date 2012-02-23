// (C) 2011-2012 Alibaba Group Holding Limited.
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU General Public License 
// version 2 as published by the Free Software Foundation. 

// Authors :fengyin <fengyin.zym@taobao.com>

require('./env');

var childs = [];
var childStatus = [];
var lastChildPos = -1;

exports.push =  function(c){
  c.on('message', msgCtrlFactory(childs.length));
  childs.push(c);
}

exports.getStatus = function(){
  return childStatus;
}

/*
 *param [object] handle 
 *dispatch the handle to childs by round-robin
 */
exports.disHandle = function(handle){
  lastChildPos++;
  if(lastChildPos >= childs.length){
    lastChildPos = 0;
  }
  childs[lastChildPos].send({'handle' : true}, handle);
}

exports.kill = function(pos){
  pos = pos || -1;
  pos = parseInt(pos);
  if(pos < 0){
    childs.forEach(function(c){
      process.kill(c.pid);
    }); 
  }else if(pos > 0 &&  pos < childs.length){
    process.kill(childs[pos].pid);
  }
}

exports.updateStatus = function(){
  childs.forEach(function(c){
    c.send({status : 'update'});
  })
}

function simpleHash(str){
  var sum = 0;
  for(var i=0; i<str.length ;i++){
    sum += str.charCodeAt(i);
  }
  return sum;
}

//distinct which child sent the message ,closure the position
function msgCtrlFactory(pos){
  return function(m){
    if(m.status){
      //log.info('get child status');
      childStatus[pos] = m.status;
    }
  }
}
