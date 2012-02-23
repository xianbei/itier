// (C) 2011-2012 Alibaba Group Holding Limited.
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU General Public License 
// version 2 as published by the Free Software Foundation. 

// Authors :fengyin <fengyin.zym@taobao.com>

function debug(str){
  //console.log(str)
}

function inspect(obj){
  //console.log(require('util').inspect(obj, false, 10));
}

/**
 * @param {string} id to identify the loader
 */
function HLoader(id){
  this._id = id;
  //in defaule cache is disabled
  this._cache = {
    "enable": false,
    "bingo" : 0,
    "items" : 0,
    "gets"  : 0,
    "timeout" : 0
  }
}

/**
 * @param {Object} options , the http options ;
 * @param {Function} cb(err ,data) ,will be called when completed
 * @param {Object} extraData ,will be passed to dataProcessing function 
 */
HLoader.prototype.load = function(options, extraData, cb){
  if(this._cache.enable){
    this._loadFromCache(options, extraData, cb);
  }else{
    this._loadFromHttp(options, extraData, cb);
  }
}

/** @param {Boolean} tf  true|fasle*/
HLoader.prototype.enableCache = function(tf){
  this._cache.enable = tf;
}

/** @param {Number} tm  the seconds*/
HLoader.prototype.setCacheTimeout = function(tm){
  tm = parseInt(tm);
  if(isNaN(tm)) tm = 0;

  this._cache.timeout = tm;
}

HLoader.prototype.getCacheStatus = function(){
  return this._cache;
}

HLoader.prototype.clearCacheStatics = function(){
  this._cache.bingo = 0;
  this._cache.gets = 0;
}

HLoader.prototype.setUrlFilter = function(fn){
  this._urlFilter = fn;
}

HLoader.prototype.setUrlHash = function(fn){
  this._urlHash = fn;
}

HLoader.prototype.setDataProcesser = function(fn){
  this._dp = fn;
}

HLoader.prototype._loadFromCache = function(options, extraData, cb){
  debug(this._id + ' driver : load from cache');
  var self = this;
  var iKey = this._id + '::' + options.path; 
  var Cache = this._cache;
  MCSkin.get(iKey, function(err, data){
    Cache["gets"]++;
    var bingo = false;
    debug("mc return");
    if(data){//cache bingo
      var dp;
      try{
        dp = self._dp(data, extraData);
        
        debug("get correct cache data " + iKey);
        Cache["bingo"]++;
        bingo = true;

        process.nextTick(function(){
          cb(null, dp);
        });
      }catch(e){
        log.error(iKey + ' error processing cached data: ');
        log.excp(e.stack);
        debug("delete : " + iKey);
        MCSkin.delete(iKey, function(err, data){
          if(err){
            log.error("delete cache : " + iKey)
            log.error(err);
          }
        });
      }   
    }
    
    //if not cache or cache data corrupt
    if(bingo == false){//end data judge
      debug("load from http");
      self._loadFromHttp(options, extraData, cb);
    }
  });
}

HLoader.prototype._setCache = function(path, value){
  
  var Cache = this._cache;
  var iKey = this._id + '::' + path;
  debug("set " + iKey + "; " + "value length :" + value.length);
  MCSkin.set(iKey, value, function(err ,data){
    if(err){
      log.error("Isearch set cache error ,key :" + i_key);
    }
  }, Cache.timeout);
}

HLoader.prototype._loadFromHttp= function(options, extraData, cb){
  debug("path : " + options.path);
  debug(this._id + ' load form http ');
  var self = this;
  var Cache = this._cache;
  http.get(options, function(res){
    var chunks = [];
    var size = 0;
    res.on("data", function(chunk){
      chunks.push(chunk);
      size += chunk.length;
    });

    res.on("end", function(){
      //console.log(chunks.length);
      if(res.statusCode != 200){
        var err = self._id + 'driver : status code error: ' + res.statusCode;
	      cb(err);
        return;
      }

      var buf ;
      if(chunks.length > 1){
        buf = new Buffer(size);
        var pos = 0;
        chunks.forEach(function(c){
          c.copy(buf, pos);
          pos += c.length;
        });
      }else{
        buf = chunks[0];
      }
      var dp ;
      try{
        dp = self._dp(buf, extraData);
      }catch(err){
		    log.error(self._id + " driver error processing reponse data");
		    log.excp(err.stack);
        //console.log(err.stack);
		    cb(err);
        return;
      }

      if(Cache.enable){
        self._setCache(options.path, buf);
      }
	    cb(null, dp);
    });

    res.on("error", function(err){
	    cb(err);
    });
  }).on('error', function(err){
    log.error(self._id + " loader " + err);
	  cb(err);
  });
}

exports.ctor = HLoader;
