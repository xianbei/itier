// (C) 2011-2012 Alibaba Group Holding Limited.
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU General Public License 
// version 2 as published by the Free Software Foundation. 

// Authors :fengyin <fengyin.zym@taobao.com>


var SqlCache = require("../src/sqlcache");

function debug(str){
  console.log(str);
}

function inspect(obj){
  console.log(require("util").inspect(obj));
}

exports.notify = function(jobId, path, jsonData){

  //check is action/config directory
  //debug(path);
  if(jobId == "ActionLoader"){
    var parr = path.split("/");
    var len = parr.length;
    if(len >= 3){
      var key = parr[len-3] + "/" + parr[len-2] +"/" + parr[len-1];
      key = key.substr(0, key.lastIndexOf("."));

      var obj = {
        path : key,
        actions : jsonData
      };
      try{
        SqlCache.set(obj);
      }catch(e){
        log.excp(e.stack);
      }
      //inspect(obj);
    }
  }
}
