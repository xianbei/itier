// (C) 2011-2012 Alibaba Group Holding Limited.
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU General Public License 
// version 2 as published by the Free Software Foundation. 

// Authors :yixuan <yixuan.zzq@taobao.com>


var operators = {
  eq : "=",
  le : "<=",
  lt : "<",
  ge : ">=",
  gt : ">",
  ne : "<>",
}

var UrlParser = function(url){
  url = url.trim();
  this.url = url;
}

UrlParser.prototype.parse = function(){
  var tokens = this.url.split("/");
  var result = {};
  result.app = tokens[1];
  result.modules = tokens[2]+"/"+tokens[3];
  result.action = tokens[4];
  result.where = {};
  result.limit = [];
  var i = 5;
  while(i < tokens.length){
    if(tokens[i] == "where"){
      var toks = tokens[++i].split(":");
      if(operators[toks[1]] === undefined){
        throw new Error("wrong operator");
      }
      result.where[toks[0]+operators[toks[1]]] = toks[2];
    }else if(tokens[i] == "limit"){
      var toks = tokens[++i].split(",");
      result.limit.push(toks[0]);
      if(toks[1] !== undefined){result.limit.push(toks[1]);}
    }
    i++;
  }
  return result;
}

exports.create = function(url){
  return new UrlParser(url);
}
