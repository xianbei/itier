// (C) 2011-2012 Alibaba Group Holding Limited.
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU General Public License 
// version 2 as published by the Free Software Foundation. 

// Authors :sanxing <sanxing@taobao.com> 

var pro=require('./prototype.js').init();

var union=function(data){
	this.data=data;
}

union.prototype=pro;

//
union.prototype.load=function(){

	if(typeof this.data!='object' || typeof this.data.length=='undefined'){
		console.log('数据格式错误');
		return;
	}

	var _=this.data,tmp=[];

	var columns=_[0].columns;

	while(_.length){
		var t=_.shift();

		if(t.data.length){
			tmp=tmp.concat(t.data);
		}
	}
	
	return {
		'columns':columns,
		'data':tmp
	};	
}


exports.init=function(data){

	return new union(data);

}
