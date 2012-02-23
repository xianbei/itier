// (C) 2011-2012 Alibaba Group Holding Limited.
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU General Public License 
// version 2 as published by the Free Software Foundation. 

// Authors :sanxing <sanxing@taobao.com>

var Debug = function(){
	//
}

Debug.info = {};

/*
 *设置
 */
Debug.set = function(k, v){
	Debug.info[k] = v;
}

/*
 *获取
 */
Debug.get = function(){
	
	var k = '';

	if(arguments.length == 1){
		k = arguments[0];
	}

	if(k && typeof Debug.info[k] != 'undefined'){
		return Debug.info[k];
	}

	return Debug.info;
}

/*
 *全部清空
 */
Debug.clean = function(){
	Debug.info = {};
}

exports.init=Debug;
