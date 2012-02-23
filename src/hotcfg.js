// (C) 2011-2012 Alibaba Group Holding Limited.
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU General Public License 
// version 2 as published by the Free Software Foundation. 

// Authors :fengyin <fengyin.zym@taobao.com>

var Cfg = require("../core/cfgload").ctor;
var fs = require("fs");

function inspect(obj){
  console.log(require("util").inspect(obj));
}
/*
var log = {
  error : function(str){
    console.log(str);
  }
}
*/
exports.Cfg2 = Cfg2;

function Cfg2(fpath, jobId){
  this.map = {};

  var self = this;
  fs.readdir(fpath, function(err ,dnames){
    if(err){
      log.error("error read directory :" + fpath);
    }else{
      dnames.forEach(function(dname){
        if(dname[0] != "."){
          self.map[dname] = new Cfg(fpath + "/" + dname, jobId);
        }
      })
    }
  });
}

Cfg2.prototype.get = function(r_path){
  var pa = r_path.split('/');
  //inspect(pa);
  var dcfg = this.map[pa[0]];
  //inspect(dcfg);
  return dcfg.get(pa[1]);
}

exports.Cfg3 = Cfg3;

function Cfg3(fpath, jobId){
  jobId = jobId || "ActionLoader";

  this.map = {};

  var self = this;
  //console.log(fpath);    
  fs.readdir(fpath, function(err ,dnames){
    if(err){
      log.error("error read directory :" + fpath);
    }else{
      dnames.forEach(function(dname){
        if(dname[0] != "."){
          self.map[dname] = new Cfg2(fpath + "/" + dname, jobId);
        }
      })
    }
  });
}

Cfg3.prototype.get = function(r_path){
  var pa = r_path.split('/');
  //inspect(pa);
  var dcfg = this.map[pa[0]];
  pa.shift();
  //inspect(dcfg);
  return dcfg.get(pa.join('/'));
}
