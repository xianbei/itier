// (C) 2011-2012 Alibaba Group Holding Limited.
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU General Public License 
// version 2 as published by the Free Software Foundation. 

// Authors :sanxing <sanxing@taobao.com> 

var p=require('../plugin.js').init();

var sumPlugin = function(data,config){
    this.data = data;
    this.config = config;
}

sumPlugin.prototype=p;

//根据group by 字段计算求和
sumPlugin.prototype.load = function(){
    
	//没有设置求和字段返回原数据
	if(typeof this.config.fields=='undefined' || this.config.fields.length==0){
		return this.data;
	}

	var _=this;

	if(typeof this.config.groupby!='undefined' && this.config.groupby.length){
		
		//
		var result={},res=[];

		this.data.forEach(function(i){
			var key=[],k='';
			_.config.groupby.forEach(function(g){
				key.push(g + '-' + i[g]);
			})
			
			k=key.join('=');

			if(k){
				if(typeof result[k]=='undefined'){
					res.push(i);
					result[k]=res.length-1;
				}
				else{
					_.config.fields.forEach(function(f){
						res[result[k]][f]+=i[f];
					})
				}
			}
		});

		delete result;

		return res;
	}
	else{
		var result={};
		this.data.forEach(function(i){
			_.config.fields.forEach(function(j){
				if(typeof result[j]=='undefined'){
					result[j]=0;
				}
				result[j]+=i[j];
			})
		})
		
		return [result];
	}
}


exports.init = function(data,config){
    return new sumPlugin(data,config);
}
