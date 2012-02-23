// (C) 2011-2012 Alibaba Group Holding Limited.
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU General Public License 
// version 2 as published by the Free Software Foundation. 

// Authors :fengyin <fengyin.zym@taobao.com>
// the base class for http loader


var http = require('http');
var util = require('util');
var HLoader = require('./_http_cache').ctor;
var SqlParser = require("./sql").ctor;

function debug(str){
  //console.log(str);
}

function inspect(obj){
  //console.log(require("util").inspect(obj, false, 10));
}


/**
* @param {Object} dc ,format like below:
* { 
*   columns: [col1, col2],
*   data : [
*     [1, 2, 3],
*     [2, 3, 4]
*   ]
* }
*/

/** @param {Array} cols */
function columnFilter(dc, cols){
  if(cols.length == 1 && cols[0] == '*') return dc;
  var cPos = [];
  var rCols = [];
  var rData = [];
  var dCols = dc.columns;
  var dData = dc.data;

  for(var i = 0; i < cols.length ; i++){
    for(var j = 0; j < dCols.length; j++){
      if(cols[i] == dCols[j]){
        cPos.push(j);
        rCols.push(cols[i]);
      }
    }
  }
  
  inspect(cPos);
  for(var i = 0; i < dData.length; i++){
    var oArr = dData[i];
    var arr = [];
    for(var j = 0; j < cPos.length; j++){
      arr.push(oArr[cPos[j]]);
    }
    rData.push(arr);
  }

  return {
    columns : rCols,
    data : rData
  }
}

/** @param {Array} asNames */
function asNameFilter(dc, asNames){
  if(asNames.length == 1 && asNames[0] == '*') return dc;

  var rData = dc.data;
  var rCols = asNames;

  return {
    columns : rCols,
    data : rData
  }
}

/** @param {Array} orderby  [{name : 'col1' , type : "ASC"}, ...]
 */
function orderbyFilter(dc, orderby){
  if(orderby.length == 0) return dc;
  var cPos = []; 
  var dCols = dc.columns;
  var dData = dc.data;
  for(var i = 0; i < orderby.length ; i++){
    for(var j = 0; j < dCols.length; j++){
      if(orderby[i].name == dCols[j]){
        cPos.push(j);
      }
    }
  }
  debug('orderby pos');
  inspect(cPos);
  var rData = dData.sort(function(a, b){
    var cmp = 0;
    for(var i = 0; i < cPos.length; i++){
      var pos = cPos[i];
      if(a[pos] != b[pos]){
         cmp = a[pos] > b[pos]; 
         if(orderby[i].type == 'DESC') cmp = !cmp;
         break;
      }
    }
    return cmp;
  })

  return {
    columns : dCols,
    data : rData
  }
}

/** @param {Array} limit  [0,2] or []
 */
function limitsFilter(dc, limits){
  if(limits.length != 2) return dc;
  var beg = limits[0];
  var end = beg + limits[1];
  
  var dData = dc.data;
  var rData = dData.slice(beg, end);
  return {
    columns : dc.columns,
    data : rData
  }
}

/** this function maybe throw exception
 * which could be caught by HLoader
 * @param [Buffer] buf the network buf 
 * @return [Object] which should be 
 * {
 *   columns : [..],
 *   data : [[...]]
 * }
 */ 

function defaultParser(buf){

  var data = JSON.parse(buf);
  //inspect(data);
  return data;
}

function filter(data, sp){

  var cols = sp.getColNames();
  data = columnFilter(data, cols);
  debug('after column filter');
  inspect(cols);
  inspect(data);

  var asNames = sp.getAsNames();
  data = asNameFilter(data, asNames);
  debug('after asNames filter');
  inspect(asNames);
  inspect(data);
  
  var orderby = sp.getOrderBy();
  data = orderbyFilter(data, orderby);
  debug('after orderby filter');
  inspect(orderby);
  inspect(data);
  
  var limits = sp.getLimits();
  data = limitsFilter(data, limits);
  debug('after limits filter');
  inspect(limits);
  inspect(data);

  //get plugins, would be deprecated
  var plugins = sp.getPlugins();

  return {
    data : data,
    plugins : plugins
  };
}

function LoaderBase(svcId, config){
  this._hLoader = new HLoader('serviceA');
  this._dataParser = defaultParser;


  var self = this;
  this._hLoader.setDataProcesser(function(){
    return dataProcessing.apply(self, arguments);
  });

  //defautl settings
  var options = {
    host : 'localhost',
    port : 80,
    maxSockets : -1,
    pathTemplate : '/'//will be set later
  }
  if(config.host) options.host = config.host;
  if(config.port) options.port = config.port;
  if(config.maxSockets) options.maxSockets = config.maxSockets;
  if(config.pathTemplate) options.pathTemplate = config.pathTemplate;
 
  this._options = options;

  this._agent = new http.Agent({maxSockets : options.maxSockets});
}

LoaderBase.prototype.setDataParser = function(fn){
  this._dataParser = fn;
}

LoaderBase.prototype.enableCache = function(tf){
  this._hLoader.enableCache(tf);
}

LoaderBase.prototype.setCacheTimeout = function(tm){
  this._hLoader.setCacheTimeout(tm);
}

LoaderBase.prototype.getCacheStatus = function(){
  return this._hLoader.getCacheStatus();
}

LoaderBase.prototype.clearCacheStatics = function(){
  this._hLoader.clearCacheStatics();
}

//this function maybe throw exceptions
function dataProcessing(buf, extraData){
  //debug('buf and extraData');
  //inspect(buf);
  //inspect(extraData);
  var data = this._dataParser(buf);
  data = filter(data, extraData);
  debug('after processing');
  inspect(data);
  return data;
}

/**
 * @param {string} type, should be debug|release ,but not supported noew
 * @param {Object} sqlObj, passed by data control flow
 * @param {Function} cb, the callback function when completed
 */
LoaderBase.prototype.load = function(type, sqlObj, cb){
  var options = {
    host : this._options.host,
    port : this._options.port,
    agent : this._agent,
    path : this._options.pathTemplate//will be set later
  };

  debug('when paased to load');
  inspect(sqlObj);

  var sp = new SqlParser(sqlObj);
  var obj = sp.getWheres();
  for(var key in obj){
    options['path'] = options['path'].replace('{' + key + '}', obj[key]);
  }

  this._hLoader.load(options, sp, function(err, dp){
    if(err){
      cb(err, {columns : [] ,data : []}, []);
    }else{
      debug('final data:');
      inspect(dp.data);
      //ugly return format design, but I have to follow it...
      cb(null, dp.data, dp.plugins);
    }
  });
} 

exports.ctor = LoaderBase;
