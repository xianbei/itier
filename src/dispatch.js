// (C) 2011-2012 Alibaba Group Holding Limited.
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU General Public License 
// version 2 as published by the Free Software Foundation. 

// Authors :sanxing <sanxing@taobao.com> ,fengyin <fengyin@taobao.com>

var conf = require('../conf');
var SqlCache = require('./sqlcache');
var data = require('./data');

var Dispatch = function(req, res){
  this.req = req;
  this.res = res;
  this.req.url = decodeURIComponent(this.req.url);
}

Dispatch.prototype.load = function(){
  var self = this;
  var url = this.req.url;
  //console.log('dispatch ' + url);
  log.access(this.req.url); 
  var sqlInfo;
  try{
    sqlInfo = SqlCache.get(this.req.url);
  }catch(e){
    log.error("config not found:" + this.req.url)
    global.replyError(self.req, self.res, 404);
    return
  }
  
  var dObj = data.init('data', sqlInfo, url, function(ret){
    if(ret.error){
      //log.error(ret.error);
      global.replyError(self.req, self.res, 500, ret.error);
    }else{
      var res = self.res;
      res.writeHead(200, {
        'content-type' : 'application/json'
      });

      res.end(JSON.stringify(ret.data));
    }
  });

  dObj.load();
}

exports.fn = function(req, res){
	return new Dispatch(req, res);
}
