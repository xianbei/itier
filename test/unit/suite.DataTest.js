// +------------------------------------------------------------------------+
// | src/data.js单元测试                                                    |
// +------------------------------------------------------------------------+
// | Copygight (c) 2003 - 2011 Taobao.com. All Rights Reserved              |
// +------------------------------------------------------------------------+
// | Author: sanxing <sanxing@taobao.com>                                   |
// +------------------------------------------------------------------------+
// | 2011年10月17日                                                         |
// +------------------------------------------------------------------------+
var conf = require('../../conf');

var MCfg = conf.mcskin;
//var mcskin=require("mcskin");
//mcskin.init(MCfg.host, MCfg.port, MCfg.maxSockets);
//global.mcskin=mcskin;

const CFG_DIRECTORY = __dirname + "/resource/";
var Cfg3 = require("../../src/hotcfg").Cfg3;
global.cfg = new Cfg3(CFG_DIRECTORY,'ActionLoader');
var u=require('util');
var data=require(__dirname + '/../../src/data.js');
var select=require(__dirname + '/../../core/parse/select.js');
var sqlcache = require('../../src/sqlcache.js');
	
	//select b.category_id as f0,b.category_name as f00,rank(sum(a.alipay_trade_amt)) as f1 
	//from mysql.rpt_cat_info_d AS  sa 
	//inner join andes.dim_category as b 
	//on a.category_id=b.category_id 
	//where a.category_id=1101 
	//and a.thedate>="2010-10-10" 
	//and a.thedate <="2010-10-10" 
	//group by f0,f00 
	//order by f1 desc 
	//limit 5 
	//union 
	//select category_id as f0,category_name as f00,rank(sum(alipay_trade_amt)) as f1 
	//from prom.rpt_cat_info_p 
	//where category_id=1101 
	//and thedate>="2010-10-10" 
	//and thedate <="2010-10-10" 
	//group by f0,f00 
	//order by f1 desc 
	//limit 5
	var config=[
		{
			'columns':{
				'f0':{
					'dist': null,
					'expr':[
						{
							'text': 'b.category_id',
							'type': 1 
						}
					]
				},
				'f00':{
					'dist': null,
					'expr':[
						{
							'text':'b.category_name',
							'type':1
						}
					]
				},
				'f1':{
					'dist': null,
					'expr':[
						{
							'text':'rank',
							'type':4
						},
						{
							'text':'(',
							'type':8
						},
						{
							'text':'sum',
							'type':4
						},
						{
							'text':'(',
							'type':8
						},
						{
							'text':'a.alipay_trade_amt',
							'type':1
						},
						{
							'text':')',
							'type':8
						},
						{
							'text':')',
							'type':8
						}
					]
				}
			},
			'sources':{
				'a':{
					'type':'table',
					'source':'rpt_cat_info_d',
					'db':'mysql'
				}
			},
			'joinmap':{
				'b':{
					'type':'table',
					'source':'dim_category',
					'db':'andes',
					'method':'INNER JOIN',
					'where':'a.category_id = b.category_id'
				}
			},
			'where':[
				{
					'relate':1,
					'values':[
						{
							'text':1101,
							'type':2
						}
					],
					'column':{
						'text':'a.category_id',
						'type':1
					}
				},
				{
					'relate': 3,
					'values':[
						{
							'text':'2010-10-10',
							'type':3
						}
					],
					'column':{
						'text':'a.thedate',
						'type':1
					}
				},
				{
					'relate':5,
					'values':[
						{
							'text':'2010-10-10',
							'type':3
						}
					],
					'column':{
						'text':'a.thedate',
						'type':1
					}
				}
			],
			'groupby':[
				[
					{
						'text':'f0',
						'type':1
					}
				],
				[
					{
						'text':'f00',
						'type':1
					}
				]
			],
			'orderby':[
				{
					'type':2,
					'expr':[
						{
							'text':'f1',
							'type':1
						}
					]
				}
			],
			'limits':[
				{
					'text':0,
					'type':2
				},
				{
					'text':5,
					'type':2
				}
			]
		},
		{
			'columns':{
				'f0':{
					'dist': null,
					'expr':[
						{
							'text':'category_id',
							'type':1
						}
					]
				},
				'f00':{
					'dist': null,
					'expr':[
						{
							'text':'category_name',
							'type':1
						}
					]
				},
				'f1':{
					'dist': null,
					'expr':[
						{
							'text':'rank',
							'type':4
						},
						{
							'text':'(',
							'type':8
						},
						{
							'text':'sum',
							'type':4
						},
						{ 
							'text':'(',
							'type':8
						},
						{
							'text':'alipay_trade_amt',
							'type':1
						},
						{
							'text':')',
							'type':8
						},
						{ 
							'text':')',
							'type':8
						}
					]
				} 
			},
			'sources':{
				'prom.rpt_cat_info_p':{
					'type':'table',
					'source':'rpt_cat_info_p',
					'db':'prom'
				}
			},
			'joinmap':{},
			'where':[
				{
					'relate': 1,
					'values':[
						{ 
							'text':1101,
							'type':2
						}
					],
					'column':{
						'text':'category_id',
						'type':1
					}
				},
				{ 
					'relate': 3,
					'values':[
						{
							'text':'2010-10-10',
							'type':3
						}
					],
					'column':{
						'text':'thedate',
						'type':1
					}
				},
				{
					'relate': 5,
					'values':[
						{ 
							'text':'2010-10-10', 
							'type':3
						}
					],
					'column':{ 
						'text':'thedate',
						'type':1
					}
				}
			],
			'groupby':[
				[
					{
						'text':'f0',
						'type':1
					}
				],
				[
					{
						'text':'f00',
						'type':1
					}
				]
			],
			'orderby':[
				{ 
					'type':2,
					'expr':[
						{
							'text':'f1',
							'type':1
						}
					]
				}
			],
			'limits':[
				{ 
					'text':0,
					'type':2
				},
				{ 
					'text':5,
					'type':2
				}
			]
		}
	];

//临时sql
exports.test_tmpSql=function(test){
	var url='';
	var cb=function(){
		//
	}
	var myData=data.init('data',config,url,cb);

	var res=myData.tmpSql(config[0],'sources','a');
	
	var c={
		'columns':{
			'f1':{
				'dist': null,
				'expr':[
					{
						'text':'rank',
						'type':4
					},
					{
						'text':'(',
						'type':8
					},
					{
						'text':'sum',
						'type':4
					},
					{ 
						'text':'(',
						'type':8
					},
					{
						'text':'a.alipay_trade_amt',
						'type':1
					},
					{
						'text':')',
						'type':8
					},
					{ 
						'text':')',
						'type':8
					}
				]
			},
			'category_id':{
				'dist':null,
				'expr':[
					{
						'text':'a.category_id',
						'type':1
					}
				]
			}
		},
		'sources':{
			'a':{
				'type':'table',
				'source':'rpt_cat_info_d',
				'db': 'mysql'
			} 
		},
		'where':[
			{
				'relate':1,
				'values':[
					{
						'text':1101,
						'type':2
					}
				],
				'column':{
					'text':'a.category_id',
					'type':1
				}
			},
			{
				'relate':3,
				'values':[
					{
						'text':'2010-10-10',
						'type':3
					}
				],
				'column':{
					'text':'a.thedate',
					'type':1
				}
			},
			{
				'relate':5,
				'values':[
					{
						'text':'2010-10-10',
						'type':3
					}
				],
				'column':{
					'text':'a.thedate',
					'type':1
				}
			}
		],
		'groupby':[
			[
				{
					'text':'a.category_id',
					'type': 1
				}
			]
		],
		'orderby':[
			{
				'type':2,
				'expr':[
					{
						'text':'f1',
						'type':1
					}
				]
			} 
		],
		'limits':[
			{ 
				'text':0,
				'type':2
			},
			{ 
				'text':5,
				'type':2
			}
		]
	};

	var exp={};
	exp.n='a';
	exp.c=c;
	exp.t='from';
	exp.jt=1;
	
	test.deepEqual(res,exp,'临时sql解析错误');


	//解析joinmap中临时sql
	var myData2=data.init('data',config,url,cb);
	var resJoin=myData2.tmpSql(config[0],'joinmap','b');
	var expJoin={
		'jt':1,
		'jType':'INNER JOIN',
		't':'join',
		'j':{
			'n':[
				'a'
			],
			'o':{
				'b.category_id':[
					'a.category_id'
				]
			},
			'oo':[
				{
					'left':{
						'd':'a',
						'f':['a.category_id']
					},
					'right':{
						'd':'b',
						'f':['f0']
					}
				}
			]
		},
		'n':'b',
		'c':{
			'columns':{
				'f00':{
					'dist': null,
					'expr':[
						{
							'type':1,
							'text':'b.category_name'
						}
					]
				},
				'f0':{
					'dist': null,
					'expr':[
						{
							'type':1,
							'text':'b.category_id'
						}
					]
				}
			},
			'where':[],
			'limits':[],
			'groupby':[],
			'sources':{
				'b':{
					'db':'andes',
					'source':'dim_category',
					'type':'table'
				}
			},
			'orderby':[]
		}
	};

	test.deepEqual(resJoin,expJoin,'临时sql join解析错误');

	test.done();
}

//mysql数据源测试
/*
exports.test_mysql=function(test){
	
	var callback=function(res){

		if(res.error){
			console.log(error);
			test.equal(1,0,'从mysql直接获取数据 产生 error 信息');
		}
		else{
			var exp={
				'columns':['f0','f1'],
				'data':[['三省','三省']]
			};
			test.deepEqual(res.data,exp,'从mysql直接获取数据 数据错误');
		}
		test.done();
    //setTimeout(function(){
    //  process.exit(0);
    //}, 2000)
	}

	var time=function(){
		
		var url='/test/sources/test/test_mysql/where/id:eq:586';
		var config=sqlcache.get(url);
		var myData=data.init('data',config,url,callback);

		myData.load();
	}

	setTimeout(time,500);
}
*/
//service_a 数据源测试
exports.test_service_a=function(test){
	
	var callback=function(res){

		if(res.error){
			console.log(res.error);
			test.equal(1,0,'从servcie_a直接获取数据 产生 error 信息');
		}
		else{
      var exp = { 
        columns: [ 'num' ], 
        data: [ 
          [ 8 ], 
          [ 6 ] 
        ] 
      }; 
			test.deepEqual(res.data,exp,'从service_a直接获取数据 数据错误');
		}
		test.done();
    process.nextTick(function(){
      process.exit(0);
    });
	}
		
		var url='/test/sources/test/test_service_a/where/fm:eq:f';
		var config=sqlcache.get(url);
      
    //console.log(util.inspect(config,false,10));
		var myData=data.init('data',config,url,callback);
		myData.load();
}
/*
exports.tearDown = function(cb){
  //test.done();
  process.nextTick(function(){
    process.exit(0);
  });
  cb();
}
*/
