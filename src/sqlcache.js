// (C) 2011-2012 Alibaba Group Holding Limited.
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU General Public License 
// version 2 as published by the Free Software Foundation. 

// Authors :yixuan <yixuan.zzq@taobao.com>

var Select = require("../core/parse/select.js");
var util = require("util");

var operators = {
  eq : "=",
  le : "<=",
  lt : "<",
  ge : ">=",
  gt : ">",
  ne : "<>",
}

var configCache = {};

//path sql sentence
/*{{{ configParser()*/
function configParser(sql){
  var get = Select.create(sql).get();
  for(var i in get){
    for(var j in get[i].sources){
      if(get[i].sources[j].type === "sql"){
        var innerSql = get[i].sources[j].source;
        get[i].sources[j].source = configParser(innerSql);
      }
    }
    for(var j in get[i].joinmap){
      if(get[i].joinmap[j].type === "sql"){
        var innerSql = get[i].joinmap[j].source;
        get[i].joinmap[j].source = configParser(innerSql);
      }
    }
  }
  return get;
}
/*}}}*/

//get replace symbols path
/*{{{ getTokens()*/
function getTokens(path,pattern,result){
  for(var i in pattern){
    var innerPath = path + ("/" + i);
    for(var j in pattern[i].where){
      if(pattern[i].where[j].replace){
        var txt = pattern[i].where[j].replace.text;
        if(!result[txt]){
          result[txt] = [];
        }
        result[txt].push(innerPath);
        delete pattern[i].where[j];
      }
    }
    for(var j in pattern[i].sources){
      if(pattern[i].sources[j].type === "sql"){
        getTokens(innerPath + "/" + j,pattern[i].sources[j].source,result);
      }
    }
  }
}
/*}}}*/

//set action config to local cache
/*{{{ set()*/
exports.set = function(config){
  for(var i in config.actions){
    var t = i;
    var result = {};
    result.pattern = configParser(config.actions[i].config);
    for(var j in config.actions[t]){
      if(j != "config"){
        result[j] = config.actions[t][j];
      }
    }
    var res = {};
    getTokens("",result.pattern,res);
    result.symbol = res;
    config.path = (config.path.substr(0,1) !== "/") ? "/" + config.path : config.path;
    configCache[config.path + "/" + t] = result;
  }
  // console.log(util.inspect(configCache,false,10));
}
/*}}}*/

//get sql object for certain url
/*{{{ get()*/
exports.get = function(url){
  var pos = url.indexOf("where");
  var key;
  if(pos == -1){
    key = url;
  }else{
    key = url.substr(0,pos-1);
  }
  key = (key.substr(0,1) == "/") ? key : "/" + key;
  var cache = objectClone(configCache[key]);
  var tokens = url.split("/");
  var whereList = {};//save all replace symbols and each symbol has what
  var tool = Select.create("");
  var tmp;
  //url中的参数分类
  for(var i = 0;i < tokens.length;i++){
    if(tokens[i] == "where"){
      var eles = tokens[i + 1].split(":");
      //check if certain param exists
      if(!cache.params || !cache.params[eles[0]]){continue;}
      for(var j in cache.params[eles[0]]){
        var param = cache.params[eles[0]][j];
        var replaced = (param.replace !== undefined && (tmp = param.replace) !== "") ? tmp : eles[0];
        if(!whereList[param.pos]){
          whereList[param.pos] = [];
        }
        if(eles[1] === "in"){
          var ins = eles[2].split(",");
          var changed = [];
          for(var k = 0;k < ins.length;k++){
            changed.push(format(ins[k],param.type));
          }
          whereList[param.pos].push(replaced + " IN (" + changed.join(",") + ")");
        }else{
          var changed = format(eles[2],param.type);
          whereList[param.pos].push(replaced + operators[eles[1]] + changed);
        }
      }
    }else if(tokens[i] === "limit"){
      var limit = tool.parseLimit(tokens[i + 1]);
      for(var j in cache.pattern){
        cache.pattern[j].limits = limit;
      }
    }
  }
  //put url elements into cache object
  for(var i in whereList){
    var res = tool.parseWhere(whereList[i].join(" AND "));
    for(var j in cache.symbol[i]){
      var pos = cache.symbol[i][j].split("/");
      var tmp = cache.pattern;
      for(var k = 1;k < pos.length;k += 2){
        if(k+1 >= pos.length){
          for(var l in res){
            tmp[pos[k]].where.push(res[l]);
          } 
        }else{
          tmp = tmp[pos[k]].sources[pos[k+1]].source;
        }
      }
    }
  }
  return cache.pattern;
}
/*}}}*/

//check and format value to certain type
function format(val,type){
  switch(type){
    case "int":
      var res = parseInt(val);
    if(isNaN(res)){throw new Error("param cannot convert to \"int\" type");}
    return res;
    default:
    return "\"" + val + "\"";
  }
}

function objectClone(obj,preventName){ 
  if((typeof obj) === 'object' && obj !== null){ 
    var res = (!obj.sort)?{}:[]; 
    for(var i in obj){ 
      if(i != preventName) 
        res[i] = objectClone(obj[i],preventName); 
    } 
    return res; 
  }else if((typeof obj) === 'function'){ 
    return (new obj()).constructor; 
  } 
  return obj; 
}
