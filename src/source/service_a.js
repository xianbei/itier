// (C) 2011-2012 Alibaba Group Holding Limited.
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU General Public License 
// version 2 as published by the Free Software Foundation. 

// Authors :fengyin <fengyin.zym@taobao.com>
//A simiple sample for a loader driver

var LoaderBase = require('./_loader_base').ctor;

var config = {
  host : 'localhost',
  port : 3561,
  maxSockets : 20,
  pathTemplate : "/?fm={fm}"
}

var loader = new LoaderBase('serviceA', config);

//type[debug] now is not supported
exports.load = function(type, sqlObj, cb){
  loader.load(type, sqlObj, cb);
}

