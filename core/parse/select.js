// (C) 2011-2012 Alibaba Group Holding Limited.
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU General Public License 
// version 2 as published by the Free Software Foundation. 

// SQL 分析器类                                         
// Author: yixuan.zzq <yixuan.zzq@taobao.com>                    

var Lexter = require('./lexter.js');
//var Cache = require('../cache/lcache.js').create('#SQL#', 8640000);

var WHERE = {
  EQ: 1,
  GT: 2,
  GE: 3,
  LT: 4,
  LE: 5,
  NE: 6,
  IN: 7,
  NOTIN: 8,
  LIKE: 9,
  NOTLIKE: 10,
  BETWEEN: 11,
  ISNULL: 20,
  NOTNULL: 21,
};

var ORDER = {
  ASC: 1,
  DESC: 2,
};

var RELATEMAP = {
  '=': WHERE.EQ,
  '==': WHERE.EQ,
  '!=': WHERE.NE,
  '<>': WHERE.NE,
  '>': WHERE.GT,
  '>=': WHERE.GE,
  '<': WHERE.LT,
  '<=': WHERE.LE,
};

//merge elements in tks with certain key to a string with sep
function fetch(key, tks, sep) {
  if(undefined === sep){
    sep = '';
  }
  var ret = [];
  for (var i = 0; i < tks.length; ++i) {
    ret[ret.length] = tks[i][key];
  }
  return ret.join(sep);
}

//escape commas and keyword in certain string and push elements into an array
function deal(tmp) {
  var ret = [];
  for (var i = 0; i < tmp.length; i++) {
    if (tmp[i].type !== Lexter.types.KEYWORD && tmp[i].type !== Lexter.types.COMMAS) {
      ret[ret.length] = tmp[i];
    }
  }
  return ret;
}

//remove left and rigth parenthese of a string if it has
function removeParenthese(tokens){
  if(tokens.length > 1 && tokens[0].text === "(" && tokens[tokens.length-1].text === ")"){
    tokens.shift();
    tokens.pop();
    return tokens;
  }
  return tokens;
}

/* {{{ public Insight.construc() */
//construct an object for a sql and parse the sentence into tokens
var Insight = function(sql) {
  this.lexter = Lexter.create(sql);
  this.sqls = [];
  this.originalTokens = this.lexter.getAll();
  this.tokens = [];
  this.ualias = [];

  this.ualias[Lexter.types.KEYWORD] = true;
  this.ualias[Lexter.types.VARIABLE] = true;
}
/* }}} */

/*{{{ public.Insight.divSql()*/
// divide sql into sqls if it contains keyword "union"
Insight.prototype.divSql = function(){
  var sqls = this.union();
  var results = [];
  if(sqls.length > 1){
    for(var i in sqls){
      var ins = new Insight(sqls[i]);
      var get = ins.divSql();
      for(var j in get){results.push(get[j]);}
    }
  }else{
    this.tokens = sqls[0];
    results.push(this.select());
  }
  return results;
}
/*}}}*/

/* {{{ public Insight.union()*/
// divide a sql into two parts if keyword "union" exists
Insight.prototype.union = function(){
  var level = 0;
  var exp = new RegExp("^UNION$","i");
  var results = [];
  var pos = 0;
  var ifUnion = false;
  for(var i = 0;i < this.originalTokens.length;i++){
    if(this.originalTokens[i].text === "("){
      level++;
    }
    if(this.originalTokens[i].text === ")"){
      level--;
    }
    if(level === 0 && exp.test(this.originalTokens[i].text)){
      ifUnion = true;
      results.push(removeParenthese(this.originalTokens.slice(pos,i)));
      results.push(removeParenthese(this.originalTokens.slice(i+1,this.originalTokens.length)));
    }
  }
  if(ifUnion){
    return results;
  }
  results.push(removeParenthese(this.originalTokens));
  return results;

}
/*}}}*/

/* {{{ public Insight.select() */
// use tokens of a sql parse to several parts including columns, sources, where and so on
Insight.prototype.select = function() {
  if (Lexter.types.KEYWORD != this.tokens[0].type || ! (/^SELECT$/i.test(this.tokens[0].text))) {
    throw new Error("SQL command should begin with keyword *SELECT*");
  }

  return {
  /*{{{*/
  'columns': this.parseColumn(
  1, this.lexter.indexOf({
    type: Lexter.types.KEYWORD,
    text: '^FROM$',
  }), null, false),
  'sources': this.parseSource(
  1 + this.lexter.indexOf({
    type: Lexter.types.KEYWORD,
    text: '^(FROM)$',
  }), this.lexter.indexOf({
    type: Lexter.types.KEYWORD,
    text: '^(WHERE|ORDER|GROUP|LIMIT|JOIN|INNER|OUTER|CROSE|LEFT|RIGHT|NATURAL)$',
  }), {
    type: Lexter.types.COMMAS,
    text: ',',
  },
  true),
  'joinmap': this.parseJoins(
  this.lexter.indexOf({
    type: Lexter.types.KEYWORD,
    text: '^(JOIN|INNER|OUTER|CROSS|LEFT|RIGHT|NATURAL)$',
  }), this.lexter.indexOf({
    type: Lexter.types.KEYWORD,
    text: '^(WHERE|ORDER|GROUP|LIMIT)$',
  }), {
    type: Lexter.types.KEYWORD,
    text: '^(JOIN|INNER|OUTER|CROSS|LEFT|RIGHT|NATURAL)$'
  }, false),
  'where': this.parseWhere(
  this.lexter.indexOf({
    type: Lexter.types.KEYWORD,
    text: '^(WHERE)$',
  }), this.lexter.indexOf({
    type: Lexter.types.KEYWORD,
    text: '^(ORDER|GROUP|LIMIT)$',
  }), {
    type: Lexter.types.KEYWORD,
    text: '^(AND)$',
  },
  true),
  'groupby': this.parseGroupBy(
  this.lexter.indexOf({
    type: Lexter.types.KEYWORD,
    text: '^(GROUP)$',
  }), this.lexter.indexOf({
    type: Lexter.types.KEYWORD,
    text: '^(ORDER|LIMIT)$',
  }), {
    type: Lexter.types.COMMAS,
    text: ',',
  },
  true),
  'orderby': this.parseOrderBy(
  this.lexter.indexOf({
    type: Lexter.types.KEYWORD,
    text: '^(ORDER)$',
  }), this.lexter.indexOf({
    type: Lexter.types.KEYWORD,
    text: '^(LIMIT)$',
  }), {
    type: Lexter.types.COMMAS,
    text: ',',
  },
  true),
  'limits': this.parseLimit(
  1 + this.lexter.indexOf({
    type: Lexter.types.KEYWORD,
    text: '^(LIMIT)$',
  }), - 1, {
    type: Lexter.types.COMMAS,
    text: ',',
  },
  true)
  /*}}}*/
	};
}
/* }}} */

/*{{{ Insight.parseColumn()*/
// get column part of a sql
Insight.prototype.parseColumn = function(beg, end, sep, txt) {
  if (null === sep) {
    sep = {
      type: Lexter.types.COMMAS,
      text: ','
    };
  }
  var pos = 0,
      key = '',
      pre = undefined, //save the operation of a three-element group(e.g column1 as col1). pre saves keyword "as".
      tmp = [], //save a three-element group
      ret = {}; //the return object
  end = (end < 0) ? this.tokens.length: end;
  while (beg < end) {
    var dist = null;
    pos = this.lexter.indexOf(sep, beg);
    if(pos > end){
      pos = -1;
    }
    if (pos >= 0) {
      tmp = this.tokens.slice(beg, pos);
      beg = pos + 1;
    } else {
      tmp = this.tokens.slice(beg, end);
      beg = end;
    }
    pre = tmp[tmp.length - 2];
    if (undefined == pre) {
      pre = {
        type: Lexter.types.OPERATOR,
        text: '',
      };
    }
    if (pre.type === Lexter.types.KEYWORD && /^as$/i.test(pre.text)) {
      key = tmp[tmp.length - 1].text;
      key = key.split('.').pop();
      var tt = [];
      for(var i =0; i< tmp.length;i++){
        if (/^(all|distinct|distinctrow)$/i.test(tmp[i].text)) {
          dist = tmp[i];
        }else{
          tt[tt.length] = tmp[i];
        }
      }
      tmp = tt;
      pos = - 2;
    } else if (pre.type !== Lexter.types.OPERATOR && true === this.ualias[tmp[tmp.length - 1].type]) {
      key = tmp[tmp.length - 1].text;
      key = key.split('.').pop();
      if (tmp[0].type === Lexter.types.KEYWORD && /^(all|distinct|distinctrow)$/i.test(tmp[0].text)) {
        dist = tmp.shift();
        pos = 0;
      } else {
        pos = - 1;
      }
    } else {
      key = fetch('text', tmp);
      key = key.split('.').pop();
      pos = 0;
    }

    if (0 == pos) {
      ret[key] = {dist : dist,expr : tmp};
    } else {
      ret[key] = {dist : dist, expr : tmp.slice(0, pos)};
    }

    if (true === txt) {
      ret[key] = {
        dist : (null === dist) ? '' : dist.text.toUpperCase(),
        expr : fetch('text', ret[key])
      };
    }
  }
  return ret;
}
/*}}}*/

/*{{{ Insight.parseSource()*/
//get source part of a sql
Insight.prototype.parseSource = function(beg, end, sep, txt) {
  if(beg <= 0 && end <= 0){
    return [];
  }
  if (null === sep) {
    sep = {
      type: Lexter.types.COMMAS,
      text: ','
    };
  }
  var pos = 0,
      key = '',
      pre = undefined, //the same as above function
      tmp = [], //the same as above function
      ret = {}; //the same as above function
  end = (end < 0) ? this.tokens.length: end;
  while (beg < end) {
    pos = this.lexter.indexOf(sep, beg);
    if(pos > end){
      pos = -1;
    }
    if (pos >= 0) {
      tmp = this.tokens.slice(beg, pos);
      beg = pos + 1;
    } else {
      tmp = this.tokens.slice(beg, end);
      beg = end;
    }
    pre = tmp[tmp.length - 2];
    if (undefined === pre) {
      pre = {
        type: Lexter.types.OPERATOR,
        text: '',
      };
    }
    if (pre.type === Lexter.types.KEYWORD && /^as$/i.test(pre.text)) {
      key = tmp[tmp.length - 1].text;
      pos = - 2;
    } else if (pre.type !== Lexter.types.OPERATOR && true === this.ualias[tmp[tmp.length - 1].type]) {
      key = tmp[tmp.length - 1].text;
      pos = - 1;
    } else {
      key = fetch('text', tmp);
      pos = 0;
    }
    if (0 === pos) {
      ret[key] = tmp;
    } else {
      ret[key] = tmp.slice(0, pos);
    }
    
    //judge the type of source(table, action or another sql)
    if (true === txt) {
      var str = fetch('text',ret[key]);
      if(str.length > 1 && str.charAt(0) === '(' && str.charAt(str.length-1) === ')'){
        str = fetch('text',ret[key]," ");
        ret[key] = {};
        ret[key].type = "sql";
        ret[key].source = str.substr(1,str.length-2);
      }else if(str.indexOf('.') > 0 ){
        ret[key] = {};
        ret[key].type = (str.substr(0,str.indexOf('.')) === "action") ? "action" : "table";
        ret[key].source = str.substr(str.indexOf('.')+1,str.length);
        if(ret[key].type == "table"){
          ret[key].db = str.substr(0,str.indexOf('.'));
        }
      }else{
        ret[key] = {};
        ret[key].type = "table";
        ret[key].db = "";
        ret[key].source = str;
      }
    }
  }
  return ret;
}
/*}}}*/

/*{{{ Insight.parseJoins()*/
// get join part of a sql
Insight.prototype.parseJoins = function(beg, end, sep, txt) {
  var pos = 0,
      key = '',
      pre = null,
      tmp = [],
      ret = {},
      tp  = [],
      method = '';
  end = (end < 0) ? this.tokens.length: end;
  if(beg < 0 ) return ret;
  while (beg < end) {
    pos = this.lexter.indexOf(sep, beg);
    if (pos >= 0) {
      tmp = this.tokens.slice(beg, pos);
      //beg = pos + 1;
      beg = pos ;
    } else {
      tmp = this.tokens.slice(beg, end);
      beg = end;
    }
    if(tmp.length === 1){
      method += tmp[0].text.toUpperCase() + ' ';
      continue;
    }
    method += 'JOIN';
    for(var i = 0; i < tmp.length; i++){
      if(tmp[i].type === Lexter.types.KEYWORD && /^on$/i.test(tmp[i].text)){
        pos = i;
      }
    }
    for(var b = 0; b < pos; b++){
      if(tmp[b].type === Lexter.types.KEYWORD && /^as$/i.test(tmp[b].text)){
        key = tmp[b+1].text;
        break;
      }
    }
    if(key.length === 0){
      key = tmp[pos-1].text;
    }

    var str = "";
    if(tmp[1].text === "("){
      for(var i = 2;i < tmp.length; i++){
        var level = 1;
        if(tmp[i].text === "("){level++;}
        if(tmp[i].text === ")" && level === 1){
          str = fetch("text",tmp.slice(1,i+1)," ");
          break;
        }
      }
    }else{
      str = tmp[1].text;
    }

    if(str.indexOf('.') > 0 ){
      ret[key] = {};
      ret[key].type = (str.substr(0,str.indexOf('.')) === "action") ? "action" : "table";
      ret[key].source = str.substr(str.indexOf('.')+1,str.length);
      if(ret[key].type === "table"){
        ret[key].db = str.substr(0,str.indexOf('.'));
      }
    }else{
      if(str.length > 1 && str.charAt(0) === '(' && str.charAt(str.length-1) === ')'){
        ret[key] = {};
        ret[key].type = "sql";
        ret[key].source = str.substr(1,str.length-2);
      }else{
        ret[key] = {};
        ret[key].type = "table";
        ret[key].db = "";
        ret[key].source = str;
      }
    }
    ret[key].method = method;
    ret[key].where = fetch('text',tmp.slice(pos+1,tmp.length),' ');

    key = '';
    method = '';
  }
  return ret;
}
/*}}}*/

/*{{{ Insight.parseWhere()*/
//get where part of a sql
Insight.prototype.parseWhere = function(beg, end, sep, txt) {
  if (beg < 0) {
    return [];
  }
  if (beg > 0) {
    beg += 1;
  }
  if (null === sep) {
    sep = {
      type: Lexter.types.KEYWORD,
      text: 'AND'
    }
  }
  var pos = 0,
      key = '',
      pre = null,
      tmp = [],
      ret = [],
      column = null,
      partime = null,
      relate = null,
      values = null;
  var last = [];
  end = (end < 0) ? this.tokens.length: end;
  while (beg < end) {
    var not = false;
    pos = this.lexter.indexOf(sep, beg);
    if(pos > end){
      pos = -1;
    }
    if (pos >= 0) {
      tmp = this.tokens.slice(beg, pos);
      beg = pos + 1;
    } else {
      tmp = this.tokens.slice(beg, end);
      beg = end;
    }
    if (tmp.length < 3 && tmp.length != 1) {
      continue;
    }
    if(tmp.length == 1){
      last.push({
        replace:tmp.pop()  
      });
      continue;
    }
    column = tmp.shift();
    if (column.type === Lexter.types.COMMAS || column.type === Lexter.types.FUNCTION) {
      continue;
    }
    partime = tmp.shift();
    if (partime.type === Lexter.types.OPERATOR) {
      if (undefined === RELATEMAP[partime.text]) {
        throw new Error("Unrecognized operator");
      }
      relate = RELATEMAP[partime.text];
      values = tmp;
    } else if (partime.text.toLowerCase() === 'is') {
      while (partime = tmp.shift()) {
        if (partime.text.toLowerCase() === 'not') {
          not = true;
        }
        if (partime.text.toLowerCase() === 'null') {
          relate = (not === true) ? WHERE.NOTNULL: WHERE.ISNULL;
          values = null;
          break;
        }
      }
    } else if (partime.text.toLowerCase() === 'not') {
      partime = tmp.shift();
      if (partime.text.toLowerCase() === 'like') {
        relate = WHERE.NOTLIKE;
        values = tmp;
      } else if (partime.text.toLowerCase() === 'in') {
        relate = WHERE.NOTIN;
        values = deal(tmp);
      } else {
        throw new Error();
      }
    } else if (partime.text.toLowerCase() === 'like') {
      relate = WHERE.LIKE;
      values = tmp;
    } else if (partime.text.toLowerCase() === 'in') {
      relate = WHERE.IN;
      values = deal(tmp);
    } else if (partime.text.toLowerCase() === 'between') {
      relate = WHERE.BETWEEN;
      values = deal(tmp);
    } else {
      //throw new Error();
    }
    ret[ret.length] = {
      relate: relate,
      values: values,
      column: column,
    };
  }
  for(var i in last){
    ret[ret.length] = last[i];
  }
  return ret;
}
/*}}}*/

/*{{{ Insight.parseGroupBy()*/
//get groupby part of a sql
Insight.prototype.parseGroupBy = function(beg, end, sep, txt) {
  if (beg < 0) {
    return [];
  }
  if (null === sep) {
    sep = {
      type: Lexter.types.COMMAS,
      text: ','
    };
  }
  var pos = 0,
      key = '',
      pre = null,
      tmp = [],
      ret = [];
  end = (end < 0) ? this.tokens.length: end;
  while (beg < end) {
    pos = this.lexter.indexOf(sep, beg);
    if(pos > end){
      pos = -1;
    }
    if (pos >= 0) {
      tmp = this.tokens.slice(beg, pos);
      beg = pos + 1;
    } else {
      tmp = this.tokens.slice(beg, end);
      beg = end;
    }

    if (tmp.length > 2) {
      var rt = [];
      for (var i = 0; i < tmp.length; i++) {
        if (!/^(group|by)$/i.test(tmp[i].text)) {
          rt[rt.length] = tmp[i];
        }
      }
      ret[ret.length] = rt;
    }else{
      ret[ret.length] = [tmp[0]];
    }
  }
  return ret;
}
/*}}}*/

/*{{{ Insight.parseOrderBy()*/
//get orderby part of a sql
Insight.prototype.parseOrderBy = function(beg, end, sep, txt) {
  if (beg < 0) {
    return [];
  }
  if (null === sep) {
    sep = {
      type: Lexter.types.COMMAS,
      text: ','
    };
  }
  var pos = 0,
      key = '',
      pre = null,
      tmp = [],
      ret = [];
  end = (end < 0) ? this.tokens.length: end;
  while (beg < end) {
    var define = false,
        value = ORDER.ASC;
    pos = this.lexter.indexOf(sep, beg);
    if(pos > end){
      pos = -1;
    }
    if (pos >= 0) {
      tmp = this.tokens.slice(beg, pos);
      beg = pos + 1;
    } else {
      tmp = this.tokens.slice(beg, end);
      beg = end;
    }
    define = true;
    if (tmp[tmp.length - 1].text.toLowerCase() === 'desc') {
      value = ORDER.DESC;
    } else if (tmp[tmp.length - 1].text.toLowerCase() === 'asc') {
      value = ORDER.ASC;
    } else {
      define = false;
    }
    if (define) {
      tmp.pop();
    }
    for (var i = 0; i < tmp.length; i++) {
      if (tmp[0].type === Lexter.types.KEYWORD && /^(order|by)$/i.test(tmp[0].text)) {
        tmp.shift();
      }
    }
    ret[ret.length] = {
      type: value,
      expr: tmp,
    };
  }
  return ret;
}
/*}}}*/

/*{{{ Insight.parseLimit()*/
Insight.prototype.parseLimit = function(beg, end, sep, txt) {
  if (beg === 0) {
    return [];
  }
  if (null === sep) {
    sep = {
      type: Lexter.types.COMMAS,
      text: ','
    };
  }
  var pos = 0,
      key = '',
      pre = null,
      tmp = [],
      ret = [];
  end = (end < 0) ? this.tokens.length: end;
  while (beg < end) {
    pos = this.lexter.indexOf(sep, beg);
    if(pos > end){
      pos = -1;
    }
    if (pos >= 0) {
      tmp = this.tokens.slice(beg, pos);
      beg = pos + 1;
    } else {
      tmp = this.tokens.slice(beg, end);
      beg = end;
    }
    for (var i = 0; i < tmp.length; i++) {
      ret.push(tmp[i]);
    }
  }
  if (ret.length === 1 && !isNaN(ret[0].text)) {
    ret.unshift({
      text: 0,
      type: Lexter.types.NUMBER
    });
  }else if(ret.length === 1 && isNaN(ret[0].text)){
    return [];
  }
  return ret;
}
/*}}}*/

/* {{{ private Insight._maps() */
Insight.prototype._maps = function(beg, end, sep, txt) {
  if (null === sep) {
    sep = {
      type: Lexter.types.COMMAS,
      text: ','
    };
  }

  var pos = 0,
      key = '',
      pre = undefined,
      tmp = [],
      ret = [];
  end = (end < 0) ? this.tokens.length: end;
  while (beg < end) {
    pos = this.lexter.indexOf(sep, beg);
    if (pos >= 0) {
      tmp = this.tokens.slice(beg, pos);
      beg = pos + 1;
    } else {
      tmp = this.tokens.slice(beg, end);
      beg = end;
    }

    pre = tmp[tmp.length - 2];
    if (undefined === pre) {
      pre = {
        type: Lexter.types.OPERATOR,
        text: '',
      };
    }
    if (pre.type === Lexter.types.KEYWORD && /^as$/i.test(pre.text)) {
      key = tmp[tmp.length - 1].text;
      pos = - 2;
    } else if (pre.type !== Lexter.types.OPERATOR && true === this.ualias[tmp[tmp.length - 1].type]) {
      key = tmp[tmp.length - 1].text;
      if (tmp[0].type === Lexter.types.KEYWORD && /^(all|distinct|distinctrow)$/i.test(tmp[0].text)) {
        pos = 0;
      } else {
        pos = - 1;
      }
    } else {
      key = fetch('text', tmp);
      pos = 0;
    }

    if (0 === pos) {
      ret[key] = tmp;
    } else {
      ret[key] = tmp.slice(0, pos);
    }

    if (true === txt) {
      ret[key] = fetch('text', ret[key]);
    }
  }
  return ret;
}
/* }}} */

/* {{{ public Parser.construct() */
var Parser = function(sql, val) {
  this.query = sql.toString().trim();
  this.param = val;
  this.ins = new Insight(this.query);
  this.result = null;
}
/* }}} */

/* {{{ public Parser.get() */
Parser.prototype.get = function() {
  this.result = this.ins.divSql();
  return this.result;
}
/* }}} */

/*{{{ Parser.replaceTable()*/
Parser.prototype.replaceSource = function(from, to) {
  if (!this.result) {
    this.get();
  }
  for(i in this.result){
    for (var tk in this.result[i].sources) {
      if (this.result[i]['sources'][tk].source == from) {
        this.result[i]['sources'][tk].source = to;
      }
    }
  }
}
/*}}}*/

/*{{{ Parser.parseWhere()*/
// get where part of a sql
Parser.prototype.parseWhere = function(where) {
  var lexter = Lexter.create(where);
  var elements = lexter.getAll();
  var beg = 0;
  var end = elements.length;
  var sep = {
    type: Lexter.types.KEYWORD,
    text: '^(AND)$',
  }
  var txt = true;
  if (beg > 0) {
    beg += 1;
  }
  if (null === sep) {
    sep = {
      type: Lexter.types.KEYWORD,
      text: 'AND'
    }
  }
  var pos = 0,
      key = '',
      pre = null,
      tmp = [],
      ret = [],
      column = null,
      partime = null,
      relate = null,
      values = null;
  end = (end < 0) ? elements.length: end;
  while (beg < end) {
    var not = false;
    pos = lexter.indexOf(sep, beg);
    if(pos > end){
      pos = -1;
    }
    if (pos >= 0) {
      tmp = elements.slice(beg, pos);
      beg = pos + 1;
    } else {
      tmp = elements.slice(beg, end);
      beg = end;
    }
    if (tmp.length < 3) {
      continue;
    }
    column = tmp.shift();
    if (column.type === Lexter.types.COMMAS || column.type === Lexter.types.FUNCTION) {
      continue;
    }
    partime = tmp.shift();
    if (partime.type === Lexter.types.OPERATOR) {
      if (undefined === RELATEMAP[partime.text]) {
        throw new Error("Unrecognized operator");
      }
      relate = RELATEMAP[partime.text];
      values = tmp;
    } else if (partime.text.toLowerCase() === 'is') {
      while (partime = tmp.shift()) {
        if (partime.text.toLowerCase() === 'not') {
          not = true;
        }
        if (partime.text.toLowerCase() === 'null') {
          relate = (not === true) ? WHERE.NOTNULL: WHERE.ISNULL;
          values = null;
          break;
        }
      }
    } else if (partime.text.toLowerCase() === 'not') {
      partime = tmp.shift();
      if (partime.text.toLowerCase() === 'like') {
        relate = WHERE.NOTLIKE;
        values = tmp;
      } else if (partime.text.toLowerCase() === 'in') {
        relate = WHERE.NOTIN;
        values = deal(tmp);
      } else {
        throw new Error();
      }
    } else if (partime.text.toLowerCase() === 'like') {
      relate = WHERE.LIKE;
      values = tmp;
    } else if (partime.text.toLowerCase() === 'in') {
      relate = WHERE.IN;
      values = deal(tmp);
    } else if (partime.text.toLowerCase() === 'between') {
      relate = WHERE.BETWEEN;
      values = deal(tmp);
    } else {
      //throw new Error();
    }
    ret[ret.length] = {
      relate: relate,
      values: values,
      column: column,
    };
  }
  return ret;
}
/*}}}*/

/*{{{ Parser.parseLimit()*/
// get limit part of a sql
Parser.prototype.parseLimit = function(limit) {
  var lexter = Lexter.create(limit);
  var elements = lexter.getAll();
  var beg = 0;
  var end = elements.length;
  var sep = {
    type: Lexter.types.COMMAS,
    text: ',',
  }
  var txt = true;
  if (undefined === sep) {
    sep = {
      type: Lexter.types.COMMAS,
      text: ','
    };
  }
  var pos = 0,
      key = '',
      pre = null,
      tmp = [],
      ret = [];
  end = (end < 0) ? elements.length: end;
  while (beg < end) {
    pos = lexter.indexOf(sep, beg);
    if(pos > end){
      pos = -1;
    }
    if (pos >= 0) {
      tmp = elements.slice(beg, pos);
      beg = pos + 1;
    } else {
      tmp = elements.slice(beg, end);
      beg = end;
    }
    for (var i = 0; i < tmp.length; i++) {
      ret.push(tmp[i]);
    }
  }
  if (ret.length === 1) {
    ret.unshift({
      text: 0,
      type: Lexter.types.NUMBER
    });
  }
  return ret;
}
/*}}}*/

/*{{{ Parser.replaceWhere()*/
Parser.prototype.replaceWhere = function(from, to) {
  if (!this.result) {
    this.get();
  }
  var pto = 'select .. where ' + to;
  var tres = new Parser(pto);
  tres = tres.get()[0].where;
  for(i in this.result){
    for (var tk in this.result[i]['where']) {
      if (this.result[i]['where'][tk].column.text === from) {
        delete this.result[i]['where'][tk];
      }
    }
    this.result[i]['where'] = this.result[i]['where'].concat(tres);
    var newarr = [];
    for (var tmp in this.result[i]['where']) {
      newarr.push(this.result[i]['where'][tmp]);
    }
    this.result[i]['where'] = newarr;
  }
  delete tres;
}
/*}}}*/

exports.create = function(sql, data) {
  return new Parser(sql, data);
}

exports.WHERE = WHERE;
exports.ORDER = ORDER;

