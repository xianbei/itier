// +------------------------------------------------------------------------+
// | src/join.js单元测试                                                    |
// +------------------------------------------------------------------------+
// | Copygight (c) 2003 - 2011 Taobao.com. All Rights Reserved              |
// +------------------------------------------------------------------------+
// | Author: sanxing <sanxing@taobao.com>                                   |
// +------------------------------------------------------------------------+
// | 2011年10月19日                                                         |
// +------------------------------------------------------------------------+

var join=require(__dirname + '/../../src/data/join.js');

//测试初始化
exports.test_coindex=function(test){
	
	var type='inner join';
	var on={
		'from':{
			'd':'a',
			'f':['id','aa']
		},
		'to':{
			'd':'b',
			'f':['xx','bb'],
			'a':['sex']
		}
	};
	var data={
		'a':{
			'columns':[
				'id',
				'aa',
				'name'
			],
			'data':[
				[1,44,'join'],
				[2,55,'xx']
			]
		},
		'b':{
			'columns':[
				'xx',
				'bb',
				'sex'
			],
			'data':[
				[1,44,'12'],
				[1,44,'56'],
				[2,55,'bv']
			]
		}
	};

	var __=join.init(type,on,data);
	var coindex=__.coindex(data[ on.to.d ].columns);
	var exp={
		'sex':2,
		'bb':1,
		'xx':0
	};
	test.deepEqual(coindex,exp,'error');
	test.done();
}


//测试init性能
exports.test_conindex_cost=function(test){
	
	var type='inner join';
	var on={
		'from':{
			'd':'a',
			'f':['id','aa']
		},
		'to':{
			'd':'b',
			'f':['xx','bb'],
			'a':['sex']
		}
	};
	var data={
		'a':{
			'columns':[
				'id',
				'aa',
				'name'
			],
			'data':[
				[1,44,'join'],
				[2,55,'xx']
			]
		},
		'b':{
			'columns':[
				'xx',
				'bb',
				'sex'
			],
			'data':[
				[1,44,'12'],
				[1,44,'56'],
				[2,55,'bv']
			]
		}
	};
	var i=2000;
	var time=0;
	while( i >0 ){

		var start= (new Date()).getTime();
		var __=join.init(type,on,data);
		var coindex=__.coindex( data[ on.to.d ].columns );

		var xx=(new Date()).getTime()-start;
		time+= xx;
		i--;
	}
	console.log('init 方法运行2000次,平均每次用时:' + time/2000 + 'ms');
	test.done();
}



//inner join
exports.test_join=function(test){
	//
	var type='inner join';
	var on={
		'from':{
			'd':'a',
			'f':['id','aa']
		},
		'to':{
			'd':'b',
			'f':['xx','bb'],
			'a':['sex']
		}
	};
	var data={
		'a':{
			'columns':[
				'id',
				'aa',
				'name'
			],
			'data':[
				[1,44,'join'],
				[2,55,'xx']
			]
		},
		'b':{
			'columns':[
				'xx',
				'bb',
				'sex'
			],
			'data':[
				[1,44,'12'],
				[2,55,'bv']
			]
		}
	};

	var __=join.init(type,on,data);
	var da=__.load();
	
	var exp={
		'columns':['id','aa','name','sex'],
		'data':[
			[1,44,'join','12'],
			[2,55,'xx','bv']
		]
	};

	test.deepEqual(da,exp,'inner join 错误');
	test.done();
}


//left join
/*exports.test_leftjoin=function(test){
	
	var type='left join';
	var on={
		'from':{
			'd':'a',
			'f':['id','aa']
		},
		'to':{
			'd':'b',
			'f':['xx','bb']
		}
	};
	var data={
		'a':[
			{'id':1,'aa':44,'name':'join'},
			{'id':2,'aa':55,'name':'xx'},
			{'id':3,'aa':5,'name':'xx'}
		],
		'b':[
			{'xx':1,'bb':44,'sex':'12'},
			{'xx':1,'bb':44,'sex':'56'},
			{'xx':2,'bb':55,'sex':'bv'}
		]
	};

	var __=join.init(type,on,data);
	__.load();
	
	var exp=[
		{'id':1,'aa':44,'name':'join','sex':'12'},
		{'id':1,'aa':44,'name':'join','sex':'56'},
		{'id':2,'aa':55,'name':'xx','sex':'bv'},
		{'id':3,'aa':5,'name':'xx','sex':''}
	];

	test.deepEqual(__.data,exp,'left join 错误');
	test.done();

}*/


//right join
/*8exports.test_rightjoin=function(test){
	
	var type='right join';
	var on={
		'from':{
			'd':'a',
			'f':['id','aa']
		},
		'to':{
			'd':'b',
			'f':['xx','bb']
		}
	};
	var data={
		'a':[
			{'id':1,'aa':44,'name':'join'},
			{'id':2,'aa':55,'name':'xx'},
			{'id':1,'aa':44,'name':'york'},
			{'id':3,'aa':66,'name':'uu'}
		],
		'b':[
			{'xx':1,'bb':44,'sex':'12'},
			{'xx':1,'bb':44,'sex':'56'},
			{'xx':2,'bb':55,'sex':'bv'}
		]
	};

	var __=join.init(type,on,data);
	__.load();
	
	var exp=[
		{'xx':1,'bb':44,'sex':'12','name':'join'},
		{'xx':1,'bb':44,'sex':'12','name':'york'},
		{'xx':1,'bb':44,'sex':'56','name':'join'},
		{'xx':1,'bb':44,'sex':'56','name':'york'},
		{'xx':2,'bb':55,'sex':'bv','name':'xx'}
	];

	test.deepEqual(__.data,exp,'right join 错误');
	test.done();

}*/


/*exports.test_outerjoin=function(test){
	test.done();
}*/


exports.test_klist_to=function(test){
	
	var type='left join';
	var on={
		'from':{
			'd':'a',
			'f':['id','aa']
		},
		'to':{
			'd':'b',
			'f':['xx','bb']
		}
	};
	var data={
		'a':{
			'columns':[
				'id',
				'aa',
				'name'
			],
			'data':[
				[1,44,'join'],
				[2,55,'xx'],
				[3,5,'xx']
			]
		},
		'b':{
			'columns':[
				'xx',
				'bb',
				'sex'
			],
			'data':[
				[1,44,'12'],
				[1,44,'56'],
				[2,55,'bv']
			]
		}
	};

	var __=join.init(type,on,data);
	var coindex=__.coindex(data.b.columns);
	var index=__.klist(data.b.data,coindex,on.to.f,['sex']);

	var indexExp={
		'=bb-55=xx-2': ['bv'],
		'=bb-44=xx-1': ['56']
	};
	
	test.deepEqual(index,indexExp,'to klist index exp error');
	
	test.done();

}


//测试from类型索引
exports.test_klist_from=function(test){
	
	var type='inner join';
	var on={
		'from':{
			'd':'a',
			'f':['id','aa']
		},
		'to':{
			'd':'b',
			'f':['xx','bb']
		}
	};
	var data={
		'a':{
			'columns':[
				'id',
				'aa',
				'name'
			],
			'data':[
				[1,44,'join'],
				[2,55,'xx'],
				[3,5,'xx']
			]
		},
		'b':{
			'columns':[
				'xx',
				'bb',
				'sex'
			],
			'data':[
				[1,44,'12'],
				[1,44,'56'],
				[2,55,'bv']
			]
		}
	};

	var __=join.init(type,on,data);
	var coindex=__.coindex(data.a.columns);
	var index=__.klist(data[on.from.d].data,coindex,on.from.f,['name']);

	var exp={
		"=aa-5=id-3":["xx"],
		"=aa-55=id-2":["xx"],
		"=aa-44=id-1":["join"]
	};

	test.deepEqual(index,exp,'test_klist_from error');
	
	test.done();

}


//检测left join 用时
/*exports.test_leftjoin_timecost=function(test){
	
	var on={
		'from':{
			'd':'a',
			'f':['id','aa']
		},
		'to':{
			'd':'b',
			'f':['xx','bb']
		}
	};

	console.log('2000x2000 left join start.........');
	var start=0;
	for(var i=0;i<2000;i++){
		
		var data={
			'a':[],
			'b':[]
		};

		//生成2000条数据数组
		var arr=['a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z',0,1,2,3,4,5,6,7,8,9];
	
		for(var ii=0;ii<2000;ii++){
			var key='',xx='',yy='';
			for(var j=0;j<5;j++){
				key+=arr[parseInt(Math.random()*36)];
				xx+=arr[parseInt(Math.random()*36)];
				yy+=arr[parseInt(Math.random()*36)];
			}
			var a={'id':ii,'aa':key,'name':xx};
			var b={'xx':ii,'bb':key,'sex':yy};
			data.a.push(a);
			data.b.push(b);
		}

		var start_=( new Date() ).getTime();

		var __=join.init('left join',on,data);
		__.load();
		start+=( new Date().getTime())-start_;
	}
	console.log('运行2000次,平均每次耗时:' + (start/2000)  + 'ms');
	test.done();
	
}*/


//测试inner join 耗时
exports.test_innerjoin_timecost=function(test){
	
	console.log('2000x2000 inner join start.......');
	var on={
		'from':{
			'd':'a',
			'f':['id','aa']
		},
		'to':{
			'd':'b',
			'f':['xx','bb']
		}
	};

	var start=0;
	for(var i=0;i<2000;i++){
		
		var data={
			'a':{
				'columns':[
					'id',
					'aa',
					'name'
				],
				'data':[]
			},
			'b':{
				'columns':[
					'xx',
					'bb',
					'sex'
				],
				'data':[]
			}
		};

		//生成2000条数据数组
		var arr=['a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z',0,1,2,3,4,5,6,7,8,9];
	
		for(var ii=0;ii<2000;ii++){
			var key='',xx='',yy='';
			for(var j=0;j<5;j++){
				key+=arr[parseInt(Math.random()*36)];
				xx+=arr[parseInt(Math.random()*36)];
				yy+=arr[parseInt(Math.random()*36)];
			}
			var a=[ii,key,xx];
			var b=[ii,key,yy];
			data.a.data.push(a);
			data.b.data.push(b);
		}
		var start_=( new Date() ).getTime();
		var ___=join.init('inner join',on,data);
		___.load();
		start+=( new Date().getTime())-start_;
	}
	console.log('运行2000次,平均每次耗时:' + (start/2000)  + 'ms');
	test.done();
	
}


//测试right join性能
/*exports.test_rightjoin_timeout=function(test){
	
	console.log('2000x2000 rigth join start.......');

	var on={
		'from':{
			'd':'a',
			'f':['id','aa']
		},
		'to':{
			'd':'b',
			'f':['xx','bb']
		}
	};
	
	var start=0;

	for(var i=0;i<2000;i++){
		
		data={
			'a':[],
			'b':[]
		};

		//生成2000条数据数组
		var arr=['a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z',0,1,2,3,4,5,6,7,8,9];
	
		for(var ii=0;ii<2000;ii++){
			var key='',xx='',yy='';
			for(var j=0;j<5;j++){
				key+=arr[parseInt(Math.random()*36)];
				xx+=arr[parseInt(Math.random()*36)];
				yy+=arr[parseInt(Math.random()*36)];
			}
			var a={'id':ii,'aa':key,'name':xx};
			var b={'xx':ii,'bb':key,'sex':yy};
			data.a.push(a);
			data.b.push(b);
		}
		var start_=( new Date() ).getTime();
		var ____=join.init('right join',on,data);
		____.load();
		start+=( new Date().getTime())-start_;
	}
	console.log('运行2000次,平均每次耗时:' + (start/2000)  + 'ms');

	test.done();
}*/

//测试构建klist性能
exports.test_klist_cost=function(test){
	
	console.log('2000x2000 klist start.......');
	var type='left join';
	var on={
		'from':{
			'd':'a',
			'f':['id','aa']
		},
		'to':{
			'd':'b',
			'f':['xx','bb']
		}
	};
	
	var start=0;

	for(var i=0;i<2000;i++){
		
		data={
			'a':{
				'columns':[
					'id',
					'aa',
					'name'
				],
				'data':[]
			},
			'b':{
				'columns':[
					'xx',
					'bb',
					'sex'
				],
				'data':[]
			}
		};

		//生成2000条数据数组
		var arr=['a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z',0,1,2,3,4,5,6,7,8,9];
	
		for(var ii=0;ii<2000;ii++){
			var key='',xx='',yy='';
			for(var j=0;j<5;j++){
				key+=arr[parseInt(Math.random()*36)];
				xx+=arr[parseInt(Math.random()*36)];
				yy+=arr[parseInt(Math.random()*36)];
			}
			var a=[ii,key,xx];
			var b=[ii,key,yy];

			data.a.data.push(a);
			data.b.data.push(b);
		}

		var start_=( new Date() ).getTime();
		
		var ____=join.init(type,on,data);

		var coindex=____.coindex(data[on.to.d].columns);

		____.klist(data[on.to.d].data,coindex,on.to.f,['sex']);

		start+=( new Date().getTime())-start_;
	}

	console.log('运行2000次,平均每次耗时:' + (start/2000)  + 'ms');

	test.done();
}