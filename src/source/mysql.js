// (C) 2011-2012 Alibaba Group Holding Limited.
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU General Public License 
// version 2 as published by the Free Software Foundation. 

// Authors :fengyin <fengyin.zym@taobao.com>

var _debug=require( __dirname + '/../../core/debug.js' );
var debugInfo=_debug.init;
var poolModule = require('generic-pool');
var SqlParser =  require("./sqlParser").ctor;
var Cfg = require("../../config/mysql");
var MCSkin =  require("mcskin");

var close_count = 0;
var pool = poolModule.Pool({
  name     : 'mysql',
  create   : function(callback){
    var Client = require('mysql').Client;
    var c = new Client();
    c.host = randomChoose(Cfg.serverlist);
    c.port = Cfg.port;
    c.user = Cfg.user;
    c.password = Cfg.password;
    c.database = Cfg.database;
    
    //c.connect();

    // parameter order: err, resource
    // new in 1.0.6
    callback(null, c);
  },
  destroy  : function(client){
    client.end();
    //if(++close_count % 10 == 0){
    //  console.log("close a client: " + (close_count));
    //}  
  },
  max      : Cfg.maxSockets,
  idleTimeoutMillis : Cfg.timeout,
  log : false
});

function randomChoose(arr){
  var ret = arr;
  if(Array.isArray(arr)){
    var len = arr.length;
    var pos = parseInt(Math.random()*100) % len;
    ret = arr[pos];
  }
  return ret;
}
/*@cb is is callbak(err , roww , field)
 */
function QueryTask(type,sqlObj, cb){
  this.type=type;
  this.sqlObj = sqlObj;
  this.cb = cb;
}


QueryTask.prototype.postFilter = function(rows, fields ,parser){
  console.log(rows);
  var colNames = parser.getColNames();
  var i,j;

  var records = [];

  if(colNames[0] == '*'){
    records = rows;
  }else{
    var asNames = parser.getAsNames();
    rows.forEach(function(r){
      var obj = {};
      for(i=0; i<asNames.length; i++){
        //var prop = colNames[i];
        obj[asNames[i]] = r[colNames[i]];
      }
      records.push(obj);
    })
  }

  return records;
}

const IKEY_PREFIX = "mysql:";
function keygen(sql){
  var str = IKEY_PREFIX + sql;
  return str.replace(/ /g, "%20");
}
function sqlreverse(ikey){
  var str = ikey.replace(IKEY_PREFIX, "");
  return str.replace(/%20/g, " ");
}

var Cache = {
  "enable": Cfg.cache.enable,
  "bingo" : 0,
  "items" : 0,
  "gets"  : 0,
  "timeout" : Cfg.cache.timeout
};

exports.Cache = Cache;
QueryTask.prototype.setCache = function(path, data, plugins){
  var obj = {};
  obj.data = data;
  obj.plugins = plugins;
  
  var i_key = keygen(path);
  var i_value = JSON.stringify(obj);
  //debug("set value length :" + i_value.length);
  MCSkin.set(i_key, i_value, function(err ,data){
    if(err){
      log.error("mysql set cache error ,key :" + sqlreverse(i_key));
      log.error(err);
    }
  }, Cache.timeout);
}


QueryTask.prototype.load = function(){
  var sqlStr;
  var sqlPlugins;
  var parser ;
  try{
    parser = new SqlParser(this.sqlObj);
    sqlStr = parser.getSql();
    sqlPlugins = parser.getPlugins();
	if(this.type=='debug'){
		debugInfo.set('source_mysql_sql', sqlStr);
		debugInfo.set('source_mysql_plugins', sqlPlugins);
	}
  }catch(err){
	  if(this.type=='debug'){
		debugInfo.set('source_mysql_error', err);
	  }
	  this.cb.call(null, err);
    return;
  }

  if(Cache.enable)
    this.loadFromCache(sqlStr, sqlPlugins);
  else
    this.loadFromDB(sqlStr, sqlPlugins);
} 

QueryTask.prototype.loadFromCache = function(sqlStr, sqlPlugins){
  var self = this;
  var iKey = keygen(sqlStr); 
  MCSkin.get(iKey, function(err, data){
    Cache["gets"]++;
    var bingo = false;
    //debug("mc return");
    if(data){//cache bingo
      //debug("get cache data" + iKey);
      try{
       // debug("data" +  data.length);
        var dpm = JSON.parse(data);
        //debug(dpm.toString());
        Cache["bingo"]++;
        bingo = true;
        self.cb.call(null, null, dpm.data, dpm.plugins);
      }catch(e){
        log.error("isearch cache data corrupt,ikey : " + iKey);
        log.error("data : " + data);
        //debug("delete : " + iKey);
        MCSkin.delete(iKey, function(err, data){
          if(err){
            log.error("delete isearch cache ,key : " + iKey)
            log.error(err);
          }
        });
      }
    }//end data judge

    if(bingo == false){
      //debug("load from http");
      self.loadFromDB(sqlStr, sqlPlugins);
    }
  });
}

function rows2table(rows){
  var res = {
    columns : [],
    data : []
  };

  //rows meust be an array
  var fnode = rows[0];
   
  if(fnode){
    var key;
    for(key in fnode){
      res.columns.push(key);
    }
    var ele;
    var data = res.data;
    var len = rows.length,i=0;
    while( i < len){
      fnode = rows[i];
      ele = [];
      for(key in fnode){
        ele.push(fnode[key]);
      }
      data.push(ele);
	  i++;
    }
  }

  return res;
}

QueryTask.prototype.loadFromDB = function(sqlStr, sqlPlugins){
  var self = this;
  if(self.type=='debug'){
	debugInfo.set('source_mysql_sql', sqlStr);
	debugInfo.set('source_mysql_plugins', sqlPlugins);
  }
  pool.acquire(function(err, client){
    if(err){
	  if(self.type=='debug'){
		debugInfo.set('source_mysql_error',err);
	  }
	  self.cb.call(null, err);
      return;
    }

    client.query(sqlStr, function(err, rows, fileds){
      pool.release(client);
      if(err){//error
		  if(self.type=='debug'){
			debugInfo.set('source_mysql_error', err);
		  }
		var tb = {
			columns : [],
			data : []
		};

		var plugins = {};
		self.cb.call(null, err, tb, plugins);
      }else{
		  if(self.type=='debug'){
			debugInfo.set('source_mysql_originalData',rows);
		  }
        var tb = rows2table(rows);
		if(self.type=='debug'){
			debugInfo.set('source_mysql_data', tb);
		}
        if(Cache.enable){
          self.setCache(sqlStr, tb, sqlPlugins);
        }
        self.cb.call(null, err ,tb, sqlPlugins);
      }
    });
  });
}
/*
exports.uninit = function(){
  var isEnd = false;
  while(!isEnd){
    pool.acquire(function(err, client){
      if(err) isEnd = true;
      else{
        client.end();
        log.info('mysql close a connetion');
      }
    });
  }
}
*/
//exports.ctor = QueryTask;
exports.load = function(type, sqlObj, cb){
  var qt = new QueryTask(type, sqlObj, cb);
  qt.load();
}
