// (C) 2011-2012 Alibaba Group Holding Limited.
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU General Public License 
// version 2 as published by the Free Software Foundation. 

// Authors :sanxing <sanxing@taobao.com>

var plugins=function(){
	//
}

plugins.prototype.load=function(){
	console.log('process data function,you should prototype it.....');
}

exports.init=function(){
	return new plugins();
}
