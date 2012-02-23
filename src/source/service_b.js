// (C) 2011-2012 Alibaba Group Holding Limited.
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU General Public License 
// version 2 as published by the Free Software Foundation. 

// Authors :fengyin <fengyin.zym@taobao.com>

var LoaderBase = require('./_loader_base').ctor;

var config = {
  host : 'localhost',
  port : 3562,
  maxSockets : 20,
  pathTemplate : "/?minId={minId}"
}

var loader = new LoaderBase('serviceB', config);

//type[debug] now is not supported
exports.load = function(type, sqlObj, cb){
  return loader.load(type, sqlObj, cb);
}

