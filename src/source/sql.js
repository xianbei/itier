// (C) 2011-2012 Alibaba Group Holding Limited.
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU General Public License 
// version 2 as published by the Free Software Foundation. 

// Authors :fengyin <fengyin.zym@taobao.com>

exports.ctor = Sql;

function debug(str){
  //console.log(str);
}

function inspect(obj){
  //console.log(require("util").inspect(obj));
}

function Sql(sqlObj){
  this._sqlObj = sqlObj;
}

Sql.prototype.getAsNames = function(){
  var cols = this._sqlObj.columns;
  var arr = [];
  for(var i in cols){
    arr.push(i);
  }
  return arr;
}

Sql.prototype.getColNames = function(){
  var cols = this._sqlObj.columns;
  var colNames = [];
  for(var i in cols){
    var robj = cols[i];
    var expr = robj.expr;
    var cn = '';
    expr.forEach(function(ele){
      cn += ele.text;
    });
    if(cn.lastIndexOf('.') > 0){
      cn = cn.substr(cn.lastIndexOf('.') + 1);
    }
    colNames.push(cn);
  };
  return colNames;
}

Sql.prototype.getWheres = function(){
  var ws = this._sqlObj.where;
  var obj = {};
  for(var i = 0; i< ws.length; i++){
    var key = ws[i].column.text;
    if(key.lastIndexOf('.') > 0){
      key = key.substr(key.lastIndexOf('.') + 1)
    }
    var vals = ws[i].values;
    var arr = [];
    vals.forEach(function(v){
      arr.push(v.text);
    })
    obj[key] = arr.join('');
  }

  debug('wheres :');
  inspect(obj);
  return obj;
}

Sql.prototype.getOrderBy = function(){
  var obArr = this._sqlObj.orderby;
  var res = [];
  var asNames  = this.getAsNames();
  var colNames = this.getColNames();
  for(var i = 0; i < obArr.length; i++){
    var ele = obArr[i];
    var ob = {
      type : 'ASC',
      name : ''
    };
    if(ele.type == 2) ob.type = 'DESC';

    var obExpr = ele.expr;
    var oname = "";
    for(var j = 0; j < obExpr.length; j++){
      oname += obExpr[j].text;
    }
    var index = -1;
    //sort by asname
    if(asNames.indexOf(oname) >= 0){
      ob.name = oname;
      res.push(ob);
    //sort by original colname
    }else if((index = colNames.indexOf(oname)) >= 0){
      //console.log('index : ' + index);
      ob.name = asNames[index];
      res.push(ob);
    }
  }
  return res;
}

Sql.prototype.getLimits = function(){
  var lArr = this._sqlObj.limits;
  var res = [];
  for(var i = 0; i < lArr.length; i++){
    res.push(lArr[i].text);
  }
  return res;
}

//not supported now
Sql.prototype.getPlugins = function(){
  return {};
}
