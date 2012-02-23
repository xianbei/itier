// (C) 2011-2012 Alibaba Group Holding Limited.
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU General Public License 
// version 2 as published by the Free Software Foundation. 

// Authors :sanxing <sanxing@taobao.com>

exports.ctor = SqlParser;

function SqlParser(sqlObj){
	//sql对象
	this.sqlObj = sqlObj || {};

	//插件
	this.plugins= null;
	
	//真实sql字符串
	this.sqlStr = null;
};

SqlParser.prototype.getAsNames = function(){
  var cols = this.sqlObj.columns;
  var arr = [];
  for(var i in cols){
    arr.push(i);
  }
  return arr;
}

SqlParser.prototype.getColNames = function(){
  var cols = this.sqlObj.columns;
  var colNames = [];
  for(var i in cols){
    var robj = cols[i];
    var expr = robj.expr;
    var cn = "";
    expr.forEach(function(ele){
      cn += ele.text;
    });
    if(cn.lastIndexOf(".") > 0){
      cn = cn.substr(cn.lastIndexOf(".") + 1);
    }
    colNames.push(cn);
  };
  return colNames;
}

SqlParser.prototype.getSql = function(){
  if(!(this.sqlStr))
    this._parse();
  return this.sqlStr;
}

SqlParser.prototype.getPlugins = function(){
  if(!(this.sqlStr))
    this._parse();
  return this.plugins;
}
//获取真实sql
SqlParser.prototype._parse = function(){
  this.sqlStr = "";
  this.plugins = {};
	
	var _=this.sqlObj;
	var sql='SELECT';

	//columns
  

  //console.log(require("util").inspect(_));
	if(typeof _.columns!='undefined'){
		var columns=[];
		for(var x in _.columns){
      //console.log("x : " + x);
			//处理列,获取真实列及相应插件
			_.columns[x]=this._parseColumn(x,_.columns[x]);
			var tmp='';
			if(_.columns[x].dist){
				tmp+=_.columns[x].dist.text + ' ';
			}
			_.columns[x].expr.forEach(function(e){
				tmp+= e.type==3 ? '"' + e.text + '"' : e.text;
			});

			tmp+= tmp==x ? '' : ' AS ' + x;

      //console.log("will push columns :" + tmp);
			columns.push(tmp);
		}
		sql+=' ' + columns.join(', ');
	}

	//from
	if(typeof _.sources!='undefined'){
    var table=[];
		for(var t in _.sources){
      var tmp= (_.sources[t].db + '.' + _.sources[t].source) != t ?  _.sources[t].source + ' AS ' + t :  _.sources[t].source;
			table.push(tmp);
		}

		if(table.length){
			sql+= ' FROM ' + table.join(',');
		}
	}

	//joinmap
	if(typeof _.joinmap!='undefined'){
    var join='';
		for(var j in _.joinmap){
			join += _.joinmap[j].method ? ' ' + _.joinmap[j].method : ' JOIN';
			join += (_.joinmap[j].db + '.' + _.joinmap[j].source!=j ? ' ' + _.joinmap[j].source + ' AS ' + j : ' ' + j);
			join += _.joinmap[j].where ? ' ON ' + _.joinmap[j].where : ' ';
		}

		if(join!='')
			sql+=join;
	}

	//where
	if(typeof _.where!='undefined' && _.where.length){
		//
		sql+=' WHERE ';
		var wt=[];
		_.where.forEach(function(w){
			var c=w.column.text;
			var r=w.relate;
			var vt=[];

			//构建真实值
			w.values.forEach(function(v){
				var vtype=v.type;
				switch(vtype){
					case 1:
						vt.push(v.text);
						break;
					case 2:
						vt.push(parseInt(v.text));
						break;
					case 3:
						//如果是like/not like字符串按原样处理
						if(r==9 || r==10){
							vt.push( v.text )
						}else{
							vt.push('"' + v.text + '"');	
						}
						break;
					default:
						break;
				}
			})

			//构建where条件
			switch(r){
				case 1:
					r=' =##';
					wt.push( c + r.replace('##',vt[0]) );
					break;
				case 2:
					r=' >##';
					wt.push( c + r.replace('##',vt[0]) );
					break;
				case 3:
					r=' >=##';
					wt.push( c + r.replace('##',vt[0]) );
					break;
				case 4:
					r=' <##';
					wt.push( c + r.replace('##',vt[0]) );
					break;
				case 5:
					r=' <=##';
					wt.push( c + r.replace('##',vt[0]) );
					break;
				case 6:
					r=' !=##';
					wt.push( c + r.replace('##',vt[0]) );
					break;
				case 7:
					r=' IN(##)';
					wt.push( c + r.replace('##',vt.join(',')) );
					break;
				case 8:
					r=' NOT IN(##)';
					wt.push( c + r.replace('##',vt.join(',')) );
					break;
				case 9:
					r=' LIKE ##';
					wt.push( c + r.replace('##',vt[0]) );
					break;
				case 10:
					r=' NOT LIKE ##';
					wt.push( c + r.replace('##',vt[0]) );
					break;
				case 11:
					r=' BETWEEN ##';
					wt.push( c + r.replace('##',vt.join('AND')) );
					break;
			}
		});

		if(wt.length)
			sql+=wt.join(' AND ');
	}
	
	//groupby
	if(typeof _.groupby!='undefined' && _.groupby.length){
		sql+=' GROUP BY ';
		var gt=[];
		_.groupby.forEach(function(g){
			var gtt='';
			g.forEach(function(gg){
				gtt+=gg.text;
			})
			
			if(gtt!='')
				gt.push(gtt);
		});

		if(gt.length)
			sql+=gt.join(',');
	}

	//orderby
	if(typeof _.orderby!='undefined' && _.orderby.length){
		sql+=' ORDER BY ';
		var ot=[];
		_.orderby.forEach(function(o){
			var ott='';
			o.expr.forEach(function(e){
				ott+=e.text;
			});

			if(o.type==1 && ott!=''){
				ot.push(ott + ' ASC');
			}
			else if(o.type==2 && ott!=''){
				ot.push(ott + ' DESC');
			}
		});

		sql+=ot.join(',');
	}

	//limit
	if(typeof _.limits!='undefined' && _.limits.length){
		sql+=' LIMIT ';
		if(typeof _.limits[0]!='undefined'){
			sql+=_.limits[0].text;
		}

		if(typeof _.limits[1]!='undefined'){
			sql+=',' + _.limits[1].text;
		}
	}

	this.sqlStr = sql;
}

//处理sql中的聚合函数,若是mysql原生函数,则原样返回,若是插件,处理后删除
//逻辑必须是插件可以包含原生函数,原生函数不能包括插件
//n--列的名字
//f--sql解析好的一个独立的列对象
SqlParser.prototype._parseColumn = function(n,f){
	//
	if(n=='' || f=='' || typeof f.expr=='undefined'){
		return;
	}

	var expr=f.expr;
	var len=expr.length;
	if(typeof this.plugins[n]=='undefined'){
		this.plugins[n]=[];
	}

	while(len){
		var e=0;
		var t=expr[e].type;
		var x=expr[e].text.toLowerCase();
		
		if(t==4){
			switch(x){
				case 'sum':
				case 'floor':
				case 'abs':
				case 'ceil':
				case 'pow':
				case 'sin':
				case 'cos':
				case 'unix_timestamp':
				case 'sqrt':
					len=0;
					break;
				case 'rank':
					this.plugins[n].push('rank');
					expr.shift();
					expr.shift();
					expr.pop();
					len=expr.length;
					break;
				case 'percent':
					this.plugins[n].push('percent');
					expr.shift();
					expr.shift();
					expr.pop();
					len=expr.length;
					break;
				case 'log':
					this.plugins[n].push('log10');
					expr.shift();
					expr.shift();
					expr.pop();
					len=expr.length;
					break;
			}
		}
		else{
			//非聚合函数,直接返回
			break;
		}
	}

	f.expr=expr;

	return f;
}

