// (C) 2011-2012 Alibaba Group Holding Limited.
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU General Public License 
// version 2 as published by the Free Software Foundation. 

var u = require('util');
var union = require('./data/union.js');
var joinObj = require('./data/wjoin.js');
var select = require(__dirname + '/../core/parse/select.js');
var sqlcache = require('./sqlcache.js');
var preReg = new RegExp('^(.*)\\..*','i');
var urlObj = require(__dirname + '/parse/url.js');

//debug信息
var _debug = require( __dirname + '/../core/debug.js' );
var debugInfo = _debug.init;

function Data(type, sqlObj, url, callback){
	
	this.type = type;
	
	this.config = sqlObj;

	this.res = [];
	
	this.url = url;
	
	this.callback = callback;

	this.tableSelected={};

}

//加载数据
Data.prototype.load = function(){
	
	var data = [], self = this;

	function callback(res){

		if(res.error){

			var r = {};
			r.error = res.error;
			r.data = [];
			if(self.type == 'debug'){
				debugInfo.set('module_error', res.error);
			}
			self.callback.call(null, r);

		}else{
			data.push(res.data);
			
			if(self.type == 'debug'){
				debugInfo.set('module_tmp_data_' + data.length, res.data);
			}

			if(data.length == self.config.length){

				//如果需要union
				if(data.length > 1){
					var unionObj = union.init(data);
					data = unionObj.load();
					
					//union后的数据
					if(self.type == 'debug'){
						debugInfo.set('module_union_data', data);
					}

				}else{
					data = data[0];
				}
				
				//最终数据
				if(self.type == 'debug'){
					debugInfo.set('module_data', data);
				}
				
				var r = {};
				r.error = '';
				r.data = data;
				self.callback.call(null, r);
			}
		}
	}

	this.getData(this.config, callback);
}


//处理数据
Data.prototype.getData = function(config, callback){

	var self = this;

	config.forEach(function(c){

		//source类型对象
		var sourceTypeObj = self.sourceType(c);

		//只有一种表类型,且都是mysql类型
		if(sourceTypeObj.sourcesTypeNum == sourceTypeObj.sourcesDbNum == 1 && typeof sourceTypeObj.sourcesType['table'] != 'undefined' && 
			typeof sourceTypeObj.sourcesType['table']['mysql'] != 'undefined' && ( (sourceTypeObj.joinmapDbNum == sourceTypeObj.joinmapTypeNum == 1 && 
			typeof sourceTypeObj.joinmapType['table'] != 'undefined' && typeof sourceTypeObj.joinmapType['table']['mysql'] != 'undefined') 
			|| (sourceTypeObj.joinmapDbNum == 0 && sourceTypeObj.joinmapTypeNum == 0) ) ){
			
			//处理loadfrom回调
			function cbfunc(i, res){
				callback.call(null, res);
			}
			
			self.loadFrom('mysql', '', c, cbfunc);

		}else{
			
			var dataTmp = {};

			//临时sql
			var tmpSql = {};

			//临时数组间join关系
			var join = {
				'from' : [],
				'join' : []
			};

			var _join = {
				'from':[],
				'join':[]
			};
			
			var unlock = true;

			function _cb(i, res){
				dataTmp[i] = res;
				var all = true;
				for(var xx in tmpSql){
					if(typeof dataTmp[xx] == 'undefined'){
						all = false;
					}
				}
				
				if(all && unlock){
					//执行join操作,并且调用最终回调函数
					unlock = false;
					if(_join.join.length){
						
						//记录中间结果
						var joinData = {}, joinData2 = [];

						var __jj = _join.join, __jLen = __jj.length, __jL = 0;

						while( __jL < __jLen ){
							//join关系
							var j = __jj[ __jL ];
							var jj = tmpSql[ j ].j.oo;
							var jjLen__ = jj.length, jjL__=0;
							if(jjLen__ == 0){
								joinData[j] = dataTmp[j].data;
								joinData2 = dataTmp[j].data;
							}else{
								while( jjL__ < jjLen__ ){
									
									var o = jj[ jjL__ ];
									var old = o.left.d, ord = o.right.d;
									var olf = o.left.f, orf = o.right.f;
									
									if( tmpSql[ old ].jt == 0){
                    joinData[j] = dataTmp[j].data;
										joinData2 = dataTmp[j].data;
									}else if( tmpSql[ ord ].jt == 0){
                    joinData[old] = dataTmp[old].data;
										joinData2 = dataTmp[old].data;
                  }else{
										var dd = {};
										dd[old] = typeof joinData[old] != 'undefined' ? joinData[old] : dataTmp[old].data;

										dd[ord] = typeof joinData[ord] != 'undefined' ? joinData[ord] : dataTmp[ord].data;

										var _o = {};
										_o.left = [];
										_o.right = [];

										//去掉关联字段中的应用前缀
										olf.forEach(function(oof){
											if(oof.indexOf('.')!=-1){
												var _oof_=oof.split('.');
												_o.left.push( _oof_[1] );
											}
											else{
												_o.left.push( oof );
											}
										})

										orf.forEach(function(oof){
											if(oof.indexOf('.')!=-1){
												var _oof_=oof.split('.');
												_o.right.push( _oof_[1] );
											}
											else{
												_o.right.push( oof );
											}
										})

										var ggg = '';

										var _type_ = tmpSql[j].jType.toLowerCase().trim();

										if( typeof dd[old].data != 'undefined' && dd[old].data.length && typeof dd[ord].data != 'undefined' && dd[ord].data.length){
											switch(_type_){
												case 'inner join':
													ggg = joinObj.innerJoin( dd[old], dd[ord], _o);
													break;
												case 'left join':
													ggg = joinObj.leftJoin( dd[old], dd[ord], _o);
													break;
											}
											//join双方的数据都变为join后的最新数据
											joinData[old] = ggg;
											joinData[ord] = ggg;
											joinData2 = ggg;
										}
									}
									jjL__++;

								}	
							}
							__jL++;
						}

						var dataTotal = {
							'columns':[],
							'data':[]
						};

						if(typeof joinData2.columns == 'undefined' && typeof joinData2.data == 'undefined'){
							callback.call(null, {'error':'', ' data':dataTotal} );
							return;
						}

						var _length = joinData2.columns.length;
						
						var _length_ = joinData2.data.length;
						
						var i = 0,j = 0;
						
						while(i < _length){
							dataTotal.columns.push(joinData2.columns[i]);
							i++;
						}

						while(j < _length_){
							dataTotal.data.push(joinData2.data[j]);
							j++;
						}
						joinData2 = null, joinData = null;
						_length = null, _length_ = null;
						i = null, j = null;

						callback.call(null, {'error':'', 'data':dataTotal} );
					}else{
						callback.call(null, dataTmp[i]);
					}
				}
			}

			//回调处理
			function cb(i, res){
				
				dataTmp[i] = res;
				//判断其他的请求是否对其他数据源有依赖关系,然后触发请求
				while(join.from.length){
					
					var j = join.from.shift();
					var allow = true;

					if(typeof tmpSql[j].j != 'undefined' && typeof tmpSql[j].j.n != 'undefined' && tmpSql[j].j.n.length){
						var _n = tmpSql[j].j.n, _len = _n.length, _i = 0;

						while( _i < _len ){
							if(typeof dataTmp[ _n[ _i ] ] == 'undefined'){
								allow = false;
								break;
							}
							_i++;
						}
					}
					
					if(allow){
						var _c = tmpSql[j].c;
						var _type = _c.sources[j].type;
						var _db = _c.sources[j].db;

						switch(_type){
							case 'sql':
								self.parseSql(j, _c, _cb);
								break;
							case 'action':
								self.parseAction(j, _c, _cb);
								break;
							case 'table':
								var db = _db ? _db : 'mysql';
								self.loadFrom(db, j, _c, _cb);
								break;
						}
					}
				}

				while(join.join.length){
					var j = join.join.shift();

					var allow = true;
					if(typeof tmpSql[j].j.n != 'undefined' && tmpSql[j].j.n.length){
						//
						var _n = tmpSql[j].j.n, _len = _n.length, _i = 0;

						while( _i < _len ){
							if(typeof dataTmp[ _n[ _i ] ] == 'undefined'){
								allow = false;
								break;
							}
							_i++;
						}
					}
					
					if(allow){
						//根据join on条件去dataTmp中寻找需要的数据,并且添加到自己的where条件中

						var joinOn = {};
						var obj = tmpSql[j].j.o;

						for( var jj in obj){
							
							var _obj = obj[jj], _obj_len = _obj.length, _jj = 0;

							while( _jj < _obj_len ){
								//获取前缀
								var x = _obj[_jj];
								var match = preReg.exec(x);
								var name = match[1];
								//字段名
								var fName=match[2];

								//查找真实的依赖字段名
								var _columns = tmpSql[ name ].c.columns;
								for(var ii in _columns){
									
									//如果和别名相同
									if(fName==ii){
										x=ii;
									}
									else{
										var _expr_ = _columns[ii].expr, _len_ = _expr_.length, i_i = 0;

										while( i_i < _len_ ){
										
											var __e = _expr_[i_i];

											if( __e.type == 1 && __e.text == x ){
												x = ii;
												break;
											}

											i_i++;
										}
									}
								}

								if(typeof joinOn[ name ] == 'undefined'){
									joinOn[ name ] = [];
									joinOn[ name ].push([x, jj]);
								}
								_jj++;
							}
						}

						//获取join条件中的数据
						var dataJ = {};

						for(var jj in joinOn){
							if(typeof dataTmp[jj] != 'undefined'){
								
								var _joinD = joinOn[jj], _joinL = _joinD.length, _jL = 0;

								while( _jL < _joinL ){
									
									var f = _joinD[ _jL ];
									var _xx_ = f[1];
									
									dataJ[ _xx_ ] = [];

									var index = dataTmp[jj].data.columns.indexOf( f[0] );
									var _data = dataTmp[jj].data.data, _data_len = _data.length, _le_ = 0;

									while( _le_ < _data_len ){
										
										var d = _data[_le_];
										if(typeof d[ index ] != 'undefined' && d[ index ] != '' && d[ index ] != 0){
											dataJ[ _xx_ ].push(d[ index ]);
										}

										_le_++;

									}
									_jL++;
								}
							}
						}
						joinOn = null;

						//将数据拼接到join sql的where条件中
						var _where = tmpSql[j].c.where;

						for(var dd in dataJ){
							var xx = {}, d = dataJ[dd], dLen = d.length, _i = 0;
							xx.relate = 7;
							xx.values = [];
							while(_i < dLen){
								xx.values.push( {'text' : d[_i], 'type' : 3} );
								_i++;
							}
							xx.column = {'text' : dd, 'type' : 1};
							_where.push(xx);
						}

						dataJ = null;

						var _c = tmpSql[j].c;
						var _type = _c.sources[j].type;
						var _db = _c.sources[j].db;
						switch(_type){
							case 'sql':
								self.parseSql(j, _c, _cb);
								break;
							case 'action':
								self.parseAction(j, _c, _cb);
								break;
							case 'table':
								var db = _db ? _db : 'mysql';
								self.loadFrom(db, j, _c, _cb);
								break;
						}
					}

				}
			}

			//组建临时sql
			for(var s in c.sources){
				//
				var gg = self.tmpSql(c, 'sources', s);

				tmpSql[s] = gg;
				join[gg.t].push(s);
				_join[gg.t].push(s);
			}

			for(var s in c.joinmap){
				//
				var gg = self.tmpSql(c, 'joinmap', s);

				tmpSql[s] = gg;
				join[gg.t].push(s);
				_join[gg.t].push(s);
			}
			
			if(join.from.length){
				
				var j = join.from.shift();
				var _c = tmpSql[j].c;
				var _type = _c.sources[j].type;
				var _db = _c.sources[j].db;
				var __cbFun = cb;
				if(join.from.length == 0 && join.join.length == 0){
					//如果全部sql只有一个,则直接点用最终的回调
					__cbFun = _cb;
				}
				switch(_type){
					case 'sql':
						self.parseSql(j, _c, __cbFun);
						break;
					case 'action':
						self.parseAction(j, _c, __cbFun);
						break;
					case 'table':
						var db = _db ? _db : 'mysql';
						self.loadFrom(db, j, _c, __cbFun);
						break;
					default:
						break;
				}
			}
		}

	});

}

//判断resource的类型
Data.prototype.sourceType = function(c){
	
	var sourcesType = {}, joinmapType = {}, sourcesTypeNum = 0, joinmapTypeNum = 0, sourcesDbNum = 0, joinmapDbNum = 0;
	
	for(var i in c.sources){
		if(typeof sourcesType[c.sources[i].type] == 'undefined'){
			sourcesType[c.sources[i].type] = {};
			sourcesType[c.sources[i].type][c.sources[i].db] = '';
			sourcesTypeNum += 1;
			sourcesDbNum += 1;
		}else{
			if(typeof sourcesType[c.sources[i].type][c.sources[i].db] == 'undefined'){
				sourcesType[c.sources[i].type][c.sources[i].db] = '';
				sourcesDbNum += 1;
			}
		}
	}

	for(var k in c.joinmap){
		if(typeof joinmapType[c.joinmap[k].type] == 'undefined'){
			joinmapType[c.joinmap[k].type] = {};
			joinmapType[c.joinmap[k].type][c.joinmap[k].db] = '';
			joinmapTypeNum += 1;
			joinmapDbNum += 1;
		}else{
			if(typeof joinmapType[c.joinmap[k].type][c.joinmap[k].db] == 'undefined'){
				joinmapType[c.joinmap[k].type][c.joinmap[k].db] = '';
				joinmapDbNum += 1;
			}
		}
	}

	var type = {
		'sourcesType' : sourcesType,
		'joinmapType' : joinmapType,
		'sourcesTypeNum' : sourcesTypeNum,
		'joinmapTypeNum' : joinmapTypeNum,
		'sourcesDbNum' : sourcesDbNum,
		'joinmapDbNum' : joinmapDbNum
	};

	return type;
}

//sql中的表是否都有select出的字段
Data.prototype.parseColumns=function(c){
	
	var self=this;

	if(typeof c.columns != 'undefined' && typeof c.columns['*'] != 'undefined' ){
		//设置所有
		for(var s in c.sources){
			if(typeof self.tableSelected[ s ]=='undefined'){
				self.tableSelected[ s ]=1;
			}
		}

		for(var j in c.joinmap){
			if(typeof self.tableSelected[ s ]=='undefined'){
				self.tableSelected[ s ]=1;
			}
		}

	}
	else if(typeof c.columns!='undefined'){
		var _columns=c.columns;
		for(var j in _columns){
			var __c=_columns[j];
			if(typeof __c.expr!='undefined' && __c.expr.length){
				var _expr=__c.expr;
				_expr.forEach(function(e){
					if( e.type==1 ){
						var _e_=e.text.split('.');
						if(_e_.length==2 && typeof self.tableSelected[ _e_[0] ]=='undefined'){
							self.tableSelected[ _e_[0] ]=1;
						}
					}
					
				});
			}
		}
	}
}

//构建临时sql
//c--完整的sql解析对象
//t--数据表所在的对象,sources/joinmap
//i--数据表的别名
Data.prototype.tmpSql = function(c, t, i){
	
	if(typeof c[t][i] == 'undefined'){
		return {};
	}

	var self = this;

	this.parseColumns(c);

	//前缀正则
	var alias = '';

	//是否参加最后的join操作,1为参加,0为忽略
	var joinTable = 0;

	if(i != c[t][i].db + '.' + c[t][i].source){
		alias = new RegExp('^' + i + '\\.','i');
	}

	var _columns = c.columns, _joinmap = c.joinmap, _where = c.where, _groupby = c.groupby, _orderby = c.orderby, _limits = c.limits;
	var _sources = c.sources;

	//columns中是否有聚合操作
	var sum = false;

	//新字段
	var columns = {};

	//新sources
	var sources = {};

	//新where条件
	var where = [];

	//新groupby
	var groupby = [];

	//新orderby
	var orderby = [];

	//新limit
	var limits = [];

	//检查和此数据表相关的columns
	if(_columns && typeof _columns['*'] !='undefined'){
		columns = _columns;
		if(joinTable==0){
			joinTable=1;
		}
	}
	else if(_columns){
		for(var j in _columns){
			var __c=_columns[j];
			if(typeof __c.expr!='undefined' && __c.expr.length){
				var _expr=__c.expr;
				_expr.forEach(function(e){

					if( e.type==1 && ( (alias && e.text.match(alias)) || alias=='') ){
						columns[j]=__c;
						if(joinTable==0){
							joinTable=1;
						}
					}
					
				});
			}
		}
	}

	//检查joinmap中的 on 字段
	if( _joinmap){
		var tmpAA = {};//记录on条件中所有的字段
		var map = {};//所有join on字段map

		for(var j in _joinmap){
			if(typeof _joinmap[j].where != 'undefined'){
				var tmp = _joinmap[j].where.toLowerCase();
				//如果是多个条件
				var rep = new RegExp('.*?and.*');
				if(tmp.match(rep)){
					var tmpA = tmp.split('and');
					tmpA.forEach(function(a){
						var aa = a.split('=');
						var _1 = aa[0].trim();
						var _2 = aa[1].trim();
						
						if(typeof map[_1] == 'undefined'){
							map[_1] = [];	
						}
						map[_1].push(_2);
						
						if(typeof map[_2] == 'undefined'){
							map[_2] = [];
						}
						map[_2].push(_1);

						if(typeof tmpAA[_1] == 'undefined'){
							tmpAA[_1] = _1;
						}

						if(typeof tmpAA[_2] == 'undefined'){
							tmpAA[_2] = _2;
						}
					});
				}else{
					var aa = tmp.split('=');
					var _1 = aa[0].trim();
					var _2 = aa[1].trim();
					
					if(typeof map[_1] == 'undefined'){
						map[_1] = [];	
					}
					map[_1].push(_2);
					
					if(typeof map[_2] == 'undefined'){
						map[_2] = [];
					}
					map[_2].push(_1);

					if(typeof tmpAA[_1] == 'undefined'){
						tmpAA[_1] = _1;
					}
					
					if(typeof tmpAA[_2] == 'undefined'){
						tmpAA[_2] = _2;
					}
				}
			}
		}

		if(typeof _columns['*'] == 'undefined'){
			//校验tmpAA中各个字段是否在现有columns中 是否属于这个source
			for(var tt in tmpAA){
				if( (alias && tt.match(alias)) || alias == '' ){

					if(joinTable==1){
						//判断对应的join字段对应的表是否有字段在columns中
						var col=0;
						map[tt].forEach(function(t){
							var _t=t.split('.');
							if(_t.length==2 && typeof self.tableSelected[ _t[0] ] !='undefined'){
								col+=1;
							}
						})

						if(col==0){
							continue;
						}
					}

					var find = false;

					//遍历现有columns
					for(var cc in columns){
						var __ = columns[cc].expr;
						__.forEach(function(_){
							if(_.type == 1 && _.text == tt){
								find = true;
							}
						});
					}

					if(find){
						continue;
					}else{
						var _tt = tt.split('.'), tmpTT = tt;
						if( _tt.length == 2 && _tt[0] == i ){
							tmpTT = _tt[1];
						}
						columns[tmpTT] = {};
						columns[tmpTT].dist = null;
						columns[tmpTT].expr = [{'text':tt, 'type':1}];
					}
				}
			}
		}
	}

	//判断columns中是否有聚合操作
	for(var cc in columns){
		if(columns[cc].expr.length > 1){
			sum = true;
		}
	}

	//sources
	if(t == 'sources'){
		sources[i] = _sources[i];
	}else if(t == 'joinmap'){
		sources[i] = {};
		sources[i].type = _joinmap[i].type;
		sources[i].source = _joinmap[i].source;
		sources[i].db = _joinmap[i].db;
	}

	//where
	if(_where && _where.length){

		_where.forEach(function(w){
			
			if( (alias && w.column.text.match(alias)) || alias == '' ){
				where.push(w);
			}
		});
	}

	//groupby
	if(sum && _groupby){
		_groupby.forEach(function(g){
			//
			if(g.length){
			
				var gTmp = [];

				g.forEach(function(gg){
					//如果本临时sql中存在此别名
					if(typeof columns[gg.text] != 'undefined'){
						gTmp.push(gg);
					}else if(typeof _columns[gg.text] != 'undefined'){
						//如果此字段名对应的真实字段在join on条件中,
						//那么和真实字段在join on 中对应的当前resource的字段也要加到groupby中
						var realName = '', _e = _columns[gg.text].expr;
						_e.forEach(function(e){
							if(e.type == 1){
								realName = e.text;
							}
						});
						//寻找和realname相关的当前resource字段名
						if(realName && typeof map[realName] != 'undefined'){
							var _map = map[realName];
							_map.forEach(function(e){
								if( (alias && e.match(alias)) || alias == '' ){
									//伪造一个groupby对象
									var ggt = {};
									ggt.type = 1;
									ggt.text = e;
									gTmp.push(ggt);
								}
							})
						}
					}
				});

				if(gTmp.length){
					groupby.push(gTmp);
				}
			}

		})
	}

	//orderby
	if( _orderby ){
		_orderby.forEach(function(o){
			if(typeof o.expr != 'undefined' && o.expr.length){
				var _e = o.expr;
				//该排序字段在column的别名中
				if(_e[0].type == 1 && typeof columns[_e[0].text] != 'undefined'){
					orderby.push(o);
				}else if(_e[0].type == 1 && ( (alias && _e[0].text.match(alias)) || alias == '') ){
					//该字段是source的真实字段
					orderby.push(o);
				}
			}
		});
	}

	//limits
	if(_limits != 'undefined' && _limits.length){
		
		//如果source只有一个
		var num = 0;
		for(var ii in c.sources){
			if(typeof c.sources[ii] != 'undefined'){
				num++;
			}
		}
		
		for(var ii in _joinmap){
			if(typeof _joinmap[ii] != 'undefined'){
				num++;
			}
		}

		if(num == 1 || sum || t == 'sources'){
			limits = _limits;
		}
	}

	var tmpSql = {};
	if(t == 'joinmap'){
		tmpSql.jType = c[t][i].method;
	}

	tmpSql.jt = joinTable;
	//sql名字
	tmpSql.n = i;
	//sql对象信息
	tmpSql.c = {
		'columns':columns,
		'sources':sources,
		'where':where,
		'groupby':groupby,
		'orderby':orderby,
		'limits':limits
	};
	//sql类型,被join/join
  tmpSql.t = ( (sum && t == 'source') || t == 'sources') ? 'from' : 'join';
	//如果是join,设置join需要的对象信息
	if(tmpSql.t == 'join'){
		tmpSql.j = {};
		//join对象的名字
		tmpSql.j.n = [];
		//临时对象,之后删除
		tmpSql.j.tn = {};
		//join关系
		tmpSql.j.o = {};
		for(var tt in map){
			if( (alias && tt.match(alias)) || alias == '' ){
				tmpSql.j.o[tt] = map[tt];
				
				map[tt].forEach(function(t){

					var match = preReg.exec(t);

					if(match[1] && typeof tmpSql.j.tn[match[1]] == 'undefined'){
						tmpSql.j.tn[match[1]] = '';
						tmpSql.j.n.push(match[1]);
					}
				});
			}
		}

		delete tmpSql.j.tn;

		tmpSql.j.oo = [];
		tmpSql.j.n.forEach(function(n){
			var xx = {
				'left' : {
					'd' : n,
					'f' : []
				},
				'right' : {
					'd' : i,
					'f' : []
				}
			};

			for(var yy in tmpSql.j.o){
				
				tmpSql.j.o[yy].forEach(function(y){
					var yyy = y.split('.');
					if(yyy[0] == n){
						
						//查找真实的别名字段
						var realName = y;
						for(var ii in c.columns){
							c.columns[ii].expr.forEach(function(e){
								if(e.type == 1 && e.text == y){
									var _ii = ii.split('.');
									if( _ii.length == 2 && _ii[0] == n ){
										realName = _ii[1];
									}
									else if( _ii.length == 1 ){
										realName = ii;
									}
								}
							})
						}
						xx.left.f.push(realName);

						//查找真实的别名字段
						var _realName = yy;

						for(var ii in columns){
							columns[ii].expr.forEach(function(e){
								if(e.type == 1 && e.text == yy){
									var _ii = ii.split('.');

									if( _ii.length == 2 && _ii[0] == i ){
										_realName = _ii[1];
									}
									else if( _ii.length == 1 ){
										_realName = ii;
									}
								}
							})
						}
						
						xx.right.f.push(_realName);
					}
				})
			}
			
			if(xx.left.f.length && xx.right.f.length){
				tmpSql.j.oo.push(xx);
			}
		})
		
	}

	return tmpSql;
}


//加载数据
Data.prototype.loadFrom=function(t, i, c, callback){

	var self = this;

	var res = {
		'data' : [],
		'error' : ''
	};

	if(self.type == 'debug'){
		debugInfo.set('module_tmpSql_' + i, c);
	}

	var t = require('./source/' + t + '.js');

  t.load(self.type, c, function(error, data, plugins){
		
		if(error){

			if(self.type == 'debug'){
				debugInfo.set('module_error', error);
			}
			
			res.error = error;
			res.data = data;
		}else if(plugins && data){
			
			if(self.type == 'debug'){
				debugInfo.set('module_plugins_' + i, plugins);
			}

			for(var p in plugins){
				if(plugins[p].length){
					var pp = plugins[p].pop();
					var ppn = pp.name.toLowerCase();
					var ppc = pp.config;
					var plu = require(__dirname + '/plugin/' + ppn + 'Plugin.js').init(data, ppc);
					data = plu.load();
				}
			}
			res.data = data;
		}else{
			res.data = data;
		}
		
		callback.call(null, i, res);

	});
}


//解析子查询
Data.prototype.parseSql=function(s, c, callback){
	
	var res = {},rt = [],d = [], self = this;
	function cb(res){
		
		if(res.error){

			if(self.type == 'debug'){
				debugInfo.set('module_error', res.error);
			}
			
			d.push({'columns':[], 'data':[]});
		}else{
			d.push(res.data);

			if(self.type == 'debug'){
				debugInfo.set('module_sql_tmp_' + d.length, res.data);
			}
		}

		if(d.length == rt.length){
			//如果需要union
			if(d.length > 1){
				var unionObj = union.init(d);
				d = unionObj.load();
				
				if(self.type == 'debug'){
					debugInfo.set('module_sql_union_data', d);
				}
			}else{
				d = d[0];
			}

			if(self.type == 'debug'){
				debugInfo.set('module_sql_data', d);
			}
			
			var r = {};
			r.error = '';
			r.data = d;
			callback.call(null, s, r);
		}
	}

	if(typeof c.sources[s] != 'undefined' && c.sources[s].type == 'sql' && typeof c.sources[s].source != 'undefined'){
		res = c.sources[s].source;
		//解析出来的对象和原有的参数合并组成新的sql对象
		if(res.length){
			res.forEach(function(r){
				
				//columns
				var _columns={};

				if(typeof r.columns!='undefined'){
					
					if( typeof c.columns['*']!='undefined' ){
						_columns=r.columns;
					}
					//如果读取的列是*,全部继承
					else if( typeof c.columns['*']=='undefined' && typeof r.columns['*']!='undefined'){
						//此时,需要去掉c中字段的前缀,那么子action中必须只有一个数据源才可以生效
						for(var ci in c.columns){
							var _c={};
							_c.dist=c.columns[ci].dist;
							_c.expr=[];

							var cie=c.columns[ci].expr;
							cie.forEach(function(_cie_){
								if(_cie_.type!=1){
									_c.expr.push(_cie_);
								}
								else{
									if( _cie_.text.indexOf('.')!=-1 ){
										var tmp_cie_=_cie_.text.split('.');
										_cie_.text = tmp_cie_[1];
										_c.expr.push(_cie_);
									}
									else{
										_c.expr.push(_cie_);
									}
								}
							})
							_columns[ci] = _c;
						}
					}
					else{
						for(var rr in r.columns){
							
							//如果可以直接查到
							if(typeof c.columns[rr]!='undefined'){
								_columns[rr]=r.columns[rr];
							}
							else if( typeof c.columns[s + '.' + rr]!='undefined' ){
								_columns[rr]=r.columns[rr];
							}
							else{
								//或者在真实字段中可以查到
								for(var ci in c.columns){
									var cie=c.columns[ci].expr;
									cie.forEach(function(ciee){
										if(ciee.type==1){
											var _text=ciee.text.split('.');
											if(_text.length==2 && _text[0]==s && _text[1]==rr ){
												//如果ci中有前缀,去掉;如果没有,保持原样
												if(ci.indexOf( s+'.')!=-1){
													var _ci=ci.split('.');
													ci=_ci[1];
												}
												_columns[ci]=r.columns[rr];
											}
											else if(_text.length==1 && _text[0]==rr){
												//如果ci中有前缀,去掉;如果没有,保持原样
												if(ci.indexOf( s+'.')!=-1){
													var _ci=ci.split('.');
													ci=_ci[1];
												}
												_columns[ci]=r.columns[rr];
											}
										}
									})
								}
							}
						}
					}
				}

				//from/join不做任何处理

				//where
				if(typeof c.where!='undefined' && c.where.length){
					
					c.where.forEach(function(w){
						var _wtext=w.column.text.split('.'), _text_='';
						if(_wtext.length==2){
							_text_=_wtext[1];
						}
						else{
							_text_=_wtext[0];
						}

						if(typeof r.columns[_text_]!='undefined'){
							var tpmColumn=r.columns[_text_];
							var tmpExpr=tpmColumn.expr;
							tmpExpr.forEach(function(te){
								if(te.type==1){
									w.column.text=te.text;
								}
							})
						}
						
						//如果当前where条件中没有设置
						var find=false;
						if( typeof r.where!='undefined' && r.where.length){
							
							r.where.forEach(function(rw){
								if(typeof rw.column.text!='undefined' && rw.column.text==w.column.text){//如果子action中使用的是别名
									find=true;
								}
								else{//如果子action中使用的是真实字段名,如果能找到别名,find=true;否则false
									for(var _c_ in r.columns){
										var _c_expr = r.columns[_c_].expr;
										_c_expr.forEach(function(_c_e){
											if(_c_e.type==1 && _c_e.text == rw.column.text && _c_ == w.column.text){
												find=true;
											}
										})
									}
								}
							});
						}

						if(!find){
							//伪造进入where条件
							r.where.push(w);
						}

					});
				}

				//更改修改后一些别名丢失的where条件
				if( typeof r.where!='undefined' && r.where.length ){
					r.where.forEach(function(rw){
						var _rw=rw.column.text.split('.');
						if(_rw.length==2){
							//
						}
						else if( _rw.length==1 && typeof _columns[ _rw[0] ]!='undefined'){
							//寻找真实的字段名,并替换掉
							var _rr=_columns[ _rw[0] ];
							_rr.expr.forEach(function(_re){
								if(_re.type==1){
									rw.column.text=_re.text;
								}
							})
						}
					})
				}

				//groupby,忽略c中的groupby
				if(typeof r.groupby!='undefined' && r.groupby.length){
					var rr=[];
					r.groupby.forEach(function(g){
						g.forEach(function(gg){
							if(gg.type==1){
								if( typeof _columns[gg.text]!='undefined' ){
									rr.push(g);
								}else if(typeof r.columns[gg.text]!='undefined'){//如果是别名
									var _expr_=r.columns[gg.text].expr;
									_expr_.forEach(function(_e_){
										if(_e_.type==1){
											gg.text=_e_.text;
											rr.push(g);
										}
									})
								}
								else{
									
									//寻找group by 字段是真实字段名
									var find=false;
									for( var _gc in _columns ){
										var _gcx=_columns[ _gc ].expr;
										var _gcl=_gcx.length, _gci=0;
										while( _gci < _gcl ){
											if( _gcx[ _gci ].type==1 && gg.text== _gcx[ _gci ].text ){
												find=_gcx[ _gci ];
												break;
											}
											_gci++;
										}
									}

									if(find!=false){
										rr.push( [find] );
									}
								}
							}
						})
					})
					r.groupby=rr;
				}


				//orderby
				if(typeof c.orderby!='undefined' && c.orderby.length){
					
					//如果r中没有orderby
					if(typeof r.orderby=='undefined' || r.orderby.length==0){
						c.orderby.forEach(function(co){
							var tmp=[],len=co.expr.length;
							for(var i=0;i<len;i++){
								if(co.expr[i].type!=1){
									tmp.push(co.expr[i]);
								}
								else{
									var coo=co.expr[i].text.split('.');
									if(coo.length==2 && coo[0]==s){//如果此处是父级action的字段真实名字,则不做修改,因为此处的名字也是子级action中字段别名
										//查找子action中真实字段
										var realName=coo[1];
										if(typeof r.columns[ realName ]!='undefined'){
											var _rr_=r.columns[ realName ].expr;
											_rr_.forEach(function(rr__){
												if(rr__.type==1){
													realName=rr__.text;
												}
											})
										}
										co.expr[i].text=realName;
										tmp.push(co.expr[i]);
									}
									else if(coo.length==1){//如果是父级action内部的别名,需要替换成父级action字段的真实名字(其实这里和子级action的别名一样)
										if(typeof c.columns[ coo[0] ]!='undefined'){
											var c__e=c.columns[ coo[0] ].expr;
											c__e.forEach(function(_ce_){
												if(_ce_.type==1){
													var _text_='';
													if(_ce_.text.indexOf('.')!=-1){//去掉别名
														var text__=_ce_.text.split('.');
														_text_=text__[1];
													}
													else{
														_text_=_ce_.text;
													}
													if(typeof r.columns[ _text_ ]!='undefined'){
														var _rr_=r.columns[ _text_ ].expr;
														_rr_.forEach(function(rr__){
															if(rr__.type==1){
																_text_=rr__.text;
															}
														})
													}
													co.expr[i].text=_text_;
													tmp.push(co.expr[i]);
												}
											})
										}
									}
								}
							}
							co.expr=tmp;
							r.orderby.push(co);
						})
					}
					else{
						c.orderby.forEach(function(co){
							if(typeof co.expr!='undefined' && co.expr.length){
								var tmp=[],find=false;
								co.expr.forEach(function(coe){
									
									if(coe.type!=1){
										tmp.push(coe);
									}
									else{
										var coee=coe.text.split('.');
										if( coee.length==2 && coee[0]==s){
											//查找子action中真实字段
											var realName=coee[1];
											if(typeof r.columns[ realName ]!='undefined'){
												var _rr_=r.columns[ realName ].expr;
												_rr_.forEach(function(rr__){
													if(rr__.type==1){
														realName=rr__.text;
													}
												})
											}
										}
										else if( coee.length==1 ){
											//首先寻找对应的子action中的字段,然后确认是否已经设置
											var realName='';
											if(typeof c.columns[ coee[0] ]!='undefined'){
												var c__e=c.columns[ coee[0] ].expr;
												c__e.forEach(function(_ce_){
													if(_ce_.type==1){
														var _text_='';
														if(_ce_.text.indexOf('.')!=-1){//去掉别名
															var text__=_ce_.text.split('.');
															realName=text__[1];
														}
														else{
															realName=_ce_.text;
														}
													}
												})
											}
										}

										//查找子action中是否已经设置
										if(realName){
											r.orderby.forEach(function(ro){
												ro.expr.forEach(function(roe){
													if(roe.type==1 && roe.text == realName){
														coe.text=realName;
														tmp.push(coe);
														find=true;
													}
													else if(roe.type==1){
														//如果这里是真实字段名,查找对应别名
														for(var _rr_ in r.columns){
															var _rr_e=r.columns[_rr_].expr;
															_rr_e.forEach(function(_e_){
																if(_e_.type==1 && roe.text==_e_.text && _rr_!=realName){
																	coe.text=realName;
																	tmp.push(coe);
																	find=true;
																}
															})
														}
													}
												})
											})
										}
									}
								})
								
								if(!find){
									co.expr=tmp;
									r.orderby.push(co);
								}
							}
						})
					}
					
				}
				else if(typeof r.orderby!='undefined' && r.orderby.length){
					//如果c中没有order by ,而r中有,则判断r中的order by 是否在c的字段中,若不在,考虑把order by 字段替换成实际字段名
					var xx=[];
					r.orderby.forEach(function(ro){
						if( typeof ro.expr!='undefined' && ro.expr.length ){
							var tmp=[], len=ro.expr.length;
							for(var i=0;i<len;i++){
								
								if(ro.expr[i].type!=1){
									tmp.push(ro.expr[i]);
								}
								else{
									var coo=ro.expr[i].text.split('.');
									if(coo.length==2){//真实字段名,不做处理
										tmp.push(ro.expr[i]);
									}
									else{
										//用的是别名,则寻找真实字段名字
										for( var rc in r.columns){
											if(rc==ro.expr[i].text){
												tmp=r.columns[rc].expr;
											}
										}
									}
								}
							}
							if(tmp.length){
								xx.push( {'type':ro.type,'expr':tmp} );
							}
						}
					})

					if(xx.length){
						r.orderby=xx;
					}
				}

				//limits
				if(typeof c.limits!='undefined' && c.limits.length){
					var cstart=cend=0;
					var limits=[];

					if(c.limits.length==2){
						cstart=parseInt(c.limits[0].text);
						cend=parseInt(c.limits[1].text);
					}
					else if(c.limits.length==1){
						cend=parseInt(c.limits[0].text);
					}

					if(typeof r.limits!='undefined' && r.limits.length){
						var rstart=rend=0;
						if(r.limits.length==2){
							rstart=parseInt(r.limits[0].text);
							rend=parseInt(r.limits[1].text);
						}
						else if(r.limits.length==1){
							rend=parseInt(r.limits[0].text);
						}


						if(cstart > rend){
							//重新选的范围过大,忽略,不做处理
						}
						//如果新的limit总数超过了原有数据总数
						else if( cstart + cend > rend ){
							cstart+=rstart;//重新定义起始偏移位置
							cend=rstart+rend-cstart;//重新计算条数
							//重新赋值
							rstart=cstart;
							rend=cend;
						}
						else{
							cstart+=rstart;
							//重新赋值
							rstart=cstart;
							rend=cend;
						}
						limits.push({'text':rstart,'type':2});
						limits.push({'text':rend,'type':2});
						r.limits=limits;
					}else{
						r.limits=c.limits;
					}
				}

				r.columns=_columns;

				rt.push(r);
				
			});
		}
		if(self.type == 'debug'){
			debugInfo.set('module_sql_sql', rt);
		}
		this.getData(rt, cb);
	}else{
		if(self.type == 'debug'){
			debugInfo.set('module_sql_sql', rt);
		}
		this.getData(c, cb);
	}
}

//解析action
Data.prototype.parseAction = function(s, c, callback){
	
	var _ = this.parseUrl(this.url);
	
	//解析新action
	if(typeof c.sources[s] == 'undefined' || c.sources[s].type != 'action'){
		return c;
	}

	var _modules = _.o.modules.replace(/\//,'\\\/');
		
	var rex = new RegExp( _.o.app + '\\\/' + _modules + '\\\/' + _.o.action );

	var action = c.sources[s].source;
	
	if(action.indexOf('.') != -1){
		var actionArr = action.split('.');

		//替换掉请求字符串
		_.s = _.s.replace(rex, _.o.app + '/' + actionArr[0] + '/' + actionArr[1] + '/' + actionArr[2]);
		
		_.o.modules = actionArr[0] + '/' + actionArr[1];
		_.o.action = actionArr[2];
	}else{

		//替换掉请求字符串
		_.s = _.s.replace(rex, _.o.app + '/' + _.o.modules + '/' + action);

		_.o.action = action;
	}

	//获取新的sql对象
	if(this.type == 'debug'){
		debugInfo.set('module_action_url', _.s);
	}
	var sql = sqlcache.get(_.s);

	if(this.type == 'debug'){
		debugInfo.set('module_action_sql', sql);
	}
	
	//调用解析子sql的接口去解析最终sql
	c.sources[s].type = 'obj';
	c.sources[s].source = sql;

	this.parseSqlObj(s, c, callback);

}

//解析嵌套调用对象
Data.prototype.parseSqlObj=function(s, c, callback){
	
	var res = [],rt = [],d = [], self = this;
	function cb(res){
		
		if(res.error){
			if(self.type == 'debug'){
				debugInfo.set('module_error', res.error);
			}
			d.push({'columns' : [], 'data' : []});
		}else{
			d.push(res.data);

			if(self.type == 'debug'){
				debugInfo.set('module_action_tmpData_' + d.length, res.data);
			}
		}

		if(d.length == rt.length){
			//如果需要union
			if(d.length > 1){
				var unionObj = union.init(d);
				d = unionObj.load();
				if(self.type == 'debug'){
					debugInfo.set('module_action_union_data', d);
				}
			}else{
				d = d[0];
			}

			if(self.type == 'debug'){
				debugInfo.set('module_action_data', d);
			}else{
				var r = {};
				r.error = '';
				r.data = d;
				callback.call(null, s, r);
			}
 		}
	}
	
	if(typeof c.sources[s] != 'undefined' && c.sources[s].type == 'obj' && typeof c.sources[s].source != 'undefined'){
		res = c.sources[s].source;
		//解析出来的对象和原有的参数合并组成新的sql对象
		if(res.length){
			res.forEach(function(r){
				
				var _columns={};

				//columns
				if(typeof r.columns!='undefined'){
					
					if(typeof c.columns['*']!='undefined'){
						_columns=r.columns;
					}
					//如果子action读取的列是*,父级action不是
					else if(typeof r.columns['*']!='undefined' && typeof c.columns['*']=='undefined'){
						//此时,需要去掉c中字段的前缀,那么子action中必须只有一个数据源才可以生效
						for(var ci in c.columns){
							var _c={};
							_c.dist=c.columns[ci].dist;
							_c.expr=[];

							var cie=c.columns[ci].expr;
							cie.forEach(function(_cie_){
								if(_cie_.type!=1){
									_c.expr.push(_cie_);
								}
								else{
									if( _cie_.text.indexOf('.')!=-1 ){
										var tmp_cie_=_cie_.text.split('.');
										_cie_.text = tmp_cie_[1];
										_c.expr.push(_cie_);
									}
									else{
										_c.expr.push(_cie_);
									}
								}
							})
							_columns[ci] = _c;
						}
					}
					else{
						for(var rr in r.columns){
							
							//如果可以直接查到
							if(typeof c.columns[rr]!='undefined'){
								_columns[rr]=r.columns[rr];
							}
							else if( typeof c.columns[s + '.' + rr]!='undefined' ){
								_columns[rr]=r.columns[rr];
							}
							else{
								//或者在真实字段中可以查到
								for(var ci in c.columns){
									var cie=c.columns[ci].expr;
									cie.forEach(function(ciee){
										if(ciee.type==1){
											var _text=ciee.text.split('.');
											if(_text.length==2 && _text[0]==s && _text[1]==rr ){
												//如果ci中有前缀,去掉;如果没有,保持原样
												if(ci.indexOf( s+'.')!=-1){
													var _ci=ci.split('.');
													ci=_ci[1];
												}
												_columns[ci]=r.columns[rr];
											}
											else if(_text.length==1 && _text[0]==rr){
												//如果ci中有前缀,去掉;如果没有,保持原样
												if(ci.indexOf( s+'.')!=-1){
													var _ci=ci.split('.');
													ci=_ci[1];
												}
												_columns[ci]=r.columns[rr];
											}
										}
									})
								}
							}

						}
					}
				}

				//from/join不做任何处理

				//where
				if(typeof c.where!='undefined' && c.where.length){
					c.where.forEach(function(w){
						var _wtext=w.column.text.split('.'), _text_='';
						if(_wtext.length==2){
							_text_=_wtext[1];
						}
						else{
							_text_=_wtext[0];
						}
							//如果应用名不在当前sql中,并且字段名是r中的别名,则替换应用名为相应名字
						if( typeof r.columns[_text_]!='undefined'){
							var tpmColumn=r.columns[_text_];
							var tmpExpr=tpmColumn.expr;
							tmpExpr.forEach(function(te){
								if(te.type==1){
									w.column.text=te.text;
								}
							})
						}

						//如果当前where条件中没有设置
						var find=false;
						if( typeof r.where!='undefined' && r.where.length){
							
							r.where.forEach(function(rw){
								if(typeof rw.column.text!='undefined' && rw.column.text==w.column.text){//如果子action中使用的是别名
									find=true;
								}
								else{//如果子action中使用的是真实字段名,如果能找到别名,find=true;否则false
									for(var _c_ in r.columns){
										var _c_expr = r.columns[_c_].expr;
										_c_expr.forEach(function(_c_e){
											if(_c_e.type==1 && _c_e.text == rw.column.text && _c_ == w.column.text){
												find=true;
											}
										})
									}
								}
							});
						}

						if(!find){
							//伪造进入where条件
							r.where.push(w);
						}
					});
				}

				//更改修改后一些别名丢失的where条件
				if( typeof r.where!='undefined' && r.where.length ){
					r.where.forEach(function(rw){
						var _rw=rw.column.text.split('.');
						if(_rw.length==2 ){
							//
						}
						else if( _rw.length==1 && typeof _columns[ _rw[0] ]!='undefined'){
							//寻找真实的字段名,并替换掉
							var _rr=_columns[ _rw[0] ];
							_rr.expr.forEach(function(_re){
								if(_re.type==1){
									rw.column.text=_re.text;
								}
							})
						}
					})
				}

				//groupby,忽略c中的groupby
				if(typeof r.groupby!='undefined' && r.groupby.length){
					var rr=[];
					r.groupby.forEach(function(g){
						g.forEach(function(gg){
							if(gg.type==1 && typeof _columns[gg.text]!='undefined'){//如果是别名
								rr.push(g);
							}
							else if(gg.type==1 && typeof r.columns[gg.text]!='undefined'){//如果是别名
								var _expr_=r.columns[gg.text].expr;
								_expr_.forEach(function(_e_){
									if(_e_.type==1){
										gg.text=_e_.text;
										rr.push(g);
									}
								})
							}
							else if(gg.type==1){//如果是真实字段
								for(var _cc_ in _columns){
									var _expr_=_columns[_cc_].expr;
									_expr_.forEach(function(_e_){
										if(_e_.type==1 && _e_.text==gg.text){
											rr.push(g);
										}
									})
								}
							}
						})
					})
					r.groupby=rr;
				}

				//orderby
				if(typeof c.orderby!='undefined' && c.orderby.length){
					
					//如果r中没有orderby
					if(typeof r.orderby=='undefined' || r.orderby.length==0){
						c.orderby.forEach(function(co){
							var tmp=[],len=co.expr.length;
							for(var i=0;i<len;i++){
								if(co.expr[i].type!=1){
									tmp.push(co.expr[i]);
								}
								else{
									var coo=co.expr[i].text.split('.');
									if(coo.length==2 && coo[0]==s){//如果此处是父级action的字段真实名字,则不做修改,因为此处的名字也是子级action中字段别名
										//查找子action中真实字段
										var realName=coo[1];
										if(typeof r.columns[ realName ]!='undefined'){
											var _rr_=r.columns[ realName ].expr;
											_rr_.forEach(function(rr__){
												if(rr__.type==1){
													realName=rr__.text;
												}
											})
										}
										co.expr[i].text=realName;
										tmp.push(co.expr[i]);
									}
									else if(coo.length==1){//如果是父级action内部的别名,需要替换成父级action字段的真实名字(其实这里和子级action的别名一样)
										if(typeof c.columns[ coo[0] ]!='undefined'){
											var c__e=c.columns[ coo[0] ].expr;
											c__e.forEach(function(_ce_){
												if(_ce_.type==1){
													var _text_='';
													if(_ce_.text.indexOf('.')!=-1){//去掉别名
														var text__=_ce_.text.split('.');
														_text_=text__[1];
													}
													else{
														_text_=_ce_.text;
													}
													
													if(typeof r.columns[ _text_ ]!='undefined'){
														var _rr_=r.columns[ _text_ ].expr;
														_rr_.forEach(function(rr__){
															if(rr__.type==1){
																_text_=rr__.text;
															}
														})
													}
													co.expr[i].text=_text_;
													tmp.push(co.expr[i]);
												}
											})
										}
									}
								}
							}
							co.expr=tmp;
							r.orderby.push(co);
						})
					}
					else{
						c.orderby.forEach(function(co){
							if(typeof co.expr!='undefined' && co.expr.length){
								var tmp=[],find=false;
								co.expr.forEach(function(coe){
									
									if(coe.type!=1){
										tmp.push(coe);
									}
									else{
										var coee=coe.text.split('.');
										if( coee.length==2 && coee[0]==s){
											//查找子action中真实字段
											var realName=coee[1];
											if(typeof r.columns[ realName ]!='undefined'){
												var _rr_=r.columns[ realName ].expr;
												_rr_.forEach(function(rr__){
													if(rr__.type==1){
														realName=rr__.text;
													}
												})
											}
										}
										else if( coee.length==1 ){
											//首先寻找对应的子action中的字段,然后确认是否已经设置
											var realName='';
											if(typeof c.columns[ coee[0] ]!='undefined'){
												var c__e=c.columns[ coee[0] ].expr;
												c__e.forEach(function(_ce_){
													if(_ce_.type==1){
														var _text_='';
														if(_ce_.text.indexOf('.')!=-1){//去掉别名
															var text__=_ce_.text.split('.');
															realName=text__[1];
														}
														else{
															realName=_ce_.text;
														}
													}
												})
											}
										}

										//查找子action中是否已经设置
										if(realName){
											r.orderby.forEach(function(ro){
												ro.expr.forEach(function(roe){
													if(roe.type==1 && roe.text == realName){
														coe.text=realName;
														tmp.push(coe);
														find=true;
													}
													else if(roe.type==1){
														//如果这里是真实字段名,查找对应别名
														for(var _rr_ in r.columns){
															var _rr_e=r.columns[_rr_].expr;
															_rr_e.forEach(function(_e_){
																if(_e_.type==1 && roe.text==_e_.text && _rr_!=realName){
																	coe.text=realName;
																	tmp.push(coe);
																	find=true;
																}
															})
														}
													}
												})
											})
										}
									}
								})
								
								if(!find){
									co.expr=tmp;
									r.orderby.push(co);
								}
							}
						})
					}
					
				}
				else if(typeof r.orderby!='undefined' && r.orderby.length){
					//如果c中没有order by ,而r中有,则判断r中的order by 是否在c的字段中,若不在,考虑把order by 字段替换成实际字段名
					var xx=[];
					r.orderby.forEach(function(ro){
						if( typeof ro.expr!='undefined' && ro.expr.length ){
							var tmp=[], len=ro.expr.length;
							for(var i=0;i<len;i++){
								
								if(ro.expr[i].type!=1){
									tmp.push(ro.expr[i]);
								}
								else{
									var coo=ro.expr[i].text.split('.');
									if(coo.length==2){//真实字段名,不做处理
										tmp.push(ro.expr[i]);
									}
									else{
										//用的是别名,则寻找真实字段名字
										for( var rc in r.columns){
											if(rc==ro.expr[i].text){
												tmp=r.columns[rc].expr;
											}
										}
									}
								}
							}
							if(tmp.length){
								xx.push( {'type':ro.type,'expr':tmp} );
							}
						}
					})

					if(xx.length){
						r.orderby=xx;
					}
				}

				//limits
				if(typeof c.limits!='undefined' && c.limits.length){
					var cstart=cend=0;
					var limits=[];

					if(c.limits.length==2){
						cstart=parseInt(c.limits[0].text);
						cend=parseInt(c.limits[1].text);
					}
					else if(c.limits.length==1){
						cend=parseInt(c.limits[0].text);
					}

					if(typeof r.limits!='undefined' && r.limits.length){
						var rstart=rend=0;
						if(r.limits.length==2){
							rstart=parseInt(r.limits[0].text);
							rend=parseInt(r.limits[1].text);
						}
						else if(r.limits.length==1){
							rend=parseInt(r.limits[0].text);
						}


						if(cstart > rend){
							//重新选的范围过大,忽略,不做处理
						}
						//如果新的limit总数超过了原有数据总数
						else if( cstart + cend > rend ){
							cstart+=rstart;//重新定义起始偏移位置
							cend=rstart+rend-cstart;//重新计算条数
							//重新赋值
							rstart=cstart;
							rend=cend;
						}
						//如果全部相同
						else if( cstart==rstart && cend == rend ){
							
						}
						else{
							cstart+=rstart;
							//重新赋值
							rstart=cstart;
							rend=cend;
						}
						limits.push({'text':rstart,'type':2});
						limits.push({'text':rend,'type':2});
						r.limits=limits;
					}else{
						r.limits=c.limits;
					}
				}

				r.columns=_columns;

				rt.push(r);
				
			});
		}
		if(self.type == 'debug'){
			debugInfo.set('module_action_tmpSql', rt);
		}
		this.getData(rt, cb);
	}else{
		if(self.type == 'debug'){
			debugInfo.set('module_action_tmpSql', rt);
		}
		this.getData(c, cb);
	}

}

//解析请求url
Data.prototype.parseUrl=function(url){

	var reg = /(.*)\?.*/gi;
	url = url.replace(reg, "$1");

	if(url == ''){
		return {};
	}
	
	var uu = urlObj.create(url);
	
	return {'o' : uu.parse(), 's' : url};
}

exports.init = function(type, sqlObj, url, callback){
	return new Data(type, sqlObj, url, callback);
}
