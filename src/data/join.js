// (C) 2011-2012 Alibaba Group Holding Limited.
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU General Public License 
// version 2 as published by the Free Software Foundation. 

// Authors :sanxing <sanxing@taobao.com> 

const INNER_JOIN='inner join';
const OUTER_JOIN='outer join';
const LEFT_JOIN='left join';
const RIGHT_JOIN='right join';

var p=require('./prototype.js').init();

var join=function(t,o,d){
	
	this.type=t.toLowerCase().trim();
	this.on=o;
	this.data=d;
	this.index={};
	this.tof=o.to.a;
}

join.prototype=p;

//
join.prototype.load=function(){
	
	var data={};
	if(this.type!=''){
		switch(this.type){
			case INNER_JOIN:
				data=this.join();
				break;
			case OUTER_JOIN:
				data=this.outerJoin();
				break;
			case LEFT_JOIN:
				data=this.leftJoin();
				break;
			case RIGHT_JOIN:
				data=this.rightJoin();
				break;
		}
	}
	
	return data;

}

//初始化
join.prototype.coindex=function(c){
	
	var colen=c.length-1;

	var coindex={};
	
	while( colen>-1 ){
		coindex[ c[colen] ] = colen;
		colen--;
	}

	return coindex;
}


//构建klist
//data---要构建索引的数据
//coindex--要构建索引的on字段的偏移量
//fileds--要构建索引的on字段数组
//to---要保留的字段数组
join.prototype.klist=function(data,coindex,fields,to){

	var flen=fields.length-1;

	var tolen=data.length-1;

	var _tolen=to.length;

	//构建索引
	var index={};

	while( tolen>-1 ){

		var k='';
		var fflen=flen;
		var _d=data[tolen];
		
		//索引为所有on条件字段的倒序组合
		while( fflen > -1 ){
			
			var f=fields[fflen];

			k+= '=' + f + '-' + _d[ coindex[f] ];
			fflen--;
		}

		if(k && typeof index[k]=='undefined'){
			
			var i=0,__d=[];
			
			while(i<_tolen){
				__d.push( _d[ coindex[ to[i] ] ] );
				i++;
			}

			index[k]=__d;
		}

		tolen--;
	}

	return index;

}

//inner join/join
join.prototype.join=function(){

	var _f=this.on.from, _to=this.on.to;

	var _data=this.data;

	var fd=_f.d, td=_to.d;

	var _len=_f.f.length-1;
	
	var _fdata=_data[ fd ].data;

	var fo=_data[ fd ].columns;

	var to=_data[ td ].columns;

	var tolen=to.length,i=0;
	var __to=[];

	while(i<tolen){
		if(_to.f.indexOf(to[i]) < 0){
			fo.push(to[i]);
			__to.push( to[i] );
		}
		i++;
	}

	var coindex=this.coindex(fo);

	var toindex=this.coindex(to);
	
	var index=this.klist(_data[ td ].data,toindex,_to.f,__to);
	
	var data={
		'columns':fo,
		'data':[
			//
		]
	};
	
	var __data=[];

	data.columns=fo;

	var dfromlen=_fdata.length,i=0;

	while( i < dfromlen ){
		
		var df=_fdata[i];

		var k='',len=_len;

		while( len>-1 ){
			
			k+='=' + _to.f[len] + '-' + df[ coindex[ _f.f[len] ] ];

			len--;
		}

		if(k){
			if(typeof index[k]!='undefined'){
				var ddd=df.concat( index[k] );
				//var ddd=['xx','ff','dd'] ;
				__data.push( ddd );
			}
		}

		i++;
	}

	data.data=__data;
	return data;
}


//outer join
join.prototype.outerJoin=function(){
	//
}


//left join
join.prototype.leftJoin=function(){
	
	var data=this.data;
	var on=this.on;

	if(on=='' || data==''){
		return data;
	}

	var index=this.index;
	var tof=this.tof;
	var _f=on.from, _to=on.to;
	var fd=_f.d;

	var res=[], dfromlen=data[fd].length;

	while( dfromlen>-1 ){

		var df=data[fd][dfrom];
		var key=[],k='';

		var len=_f.f.length;

		for(var i=0;i<len;i++){
			key.push(_to.f[i] + '-' + df[_f.f[i]]);
		}

		k=key.join('=');

		if(k){
			if(typeof index[k]!='undefined'){
				var dklen=index[k].length;

				for(var dk=0;dk<dklen;dk++){
					var d=data[_to.d][ index[k][dk] ];

					for(var dd in d){
						df[dd]=d[dd];
					}

					res.push(df);
				}
			}
			else{
				var toflen=tof.length;
				for(var _tof=0;_tof<toflen;_tof++){
					var dd=tof[_tof];
					df[dd]='';
				}
				res.push(df);
			}
		}

		dfromlen--;
	}

	return res;
}

//right join
join.prototype.rightJoin=function(){
	//交换from和to,然后调用left join
	var from=this.on.from;
	var to=this.on.to;
	this.on.from=to;
	this.on.to=from;

	this.leftJoin();
}


exports.init=function(type,on,data){
	
	return new join(type,on,data);
}
