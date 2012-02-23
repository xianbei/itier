var SumPlugin	= require('../../src/plugin/sumPlugin.js');

exports.test_sumPlugin_nogroupby_work_fine=function(test){
    var table = [
			{a:1,b:2,c:3},
      {a:2,b:9,c:6},
      {a:7,b:6,c:11}
     ];
    var f = ["a","c"];
	var config={};
	config.fields=f;
        
	var sp = SumPlugin.init(table,config);

	var exp=[{a:10,c:20}];

    test.deepEqual(sp.load(),exp,'sum插件无 group by 时错误');
        
	test.done();
}

exports.test_sumPlugin_groupby_work_fine=function(test){
		
	var data=[
		{'id':1,'name':'aa','alipay_trade_amt':10,'alipay_trade_num':2},
		{'id':1,'name':'aa','alipay_trade_amt':20,'alipay_trade_num':3},
		{'id':2,'name':'bb','alipay_trade_amt':15,'alipay_trade_num':5},
		{'id':2,'name':'bb','alipay_trade_amt':26,'alipay_trade_num':8},
		{'id':3,'name':'cc','alipay_trade_amt':56,'alipay_trade_num':4}
	];

	var config={
		'groupby':['id','name'],
		'fields':['alipay_trade_amt','alipay_trade_num']
	}

	var sp = SumPlugin.init(data,config);

	var exp=[
		{
			'id':1,
			'name':'aa',
			'alipay_trade_amt':30,
			'alipay_trade_num':5
		},
		{
			'id':2,
			'name':'bb',
			'alipay_trade_amt':41,
			'alipay_trade_num':13
		},
		{
			'id':3,
			'name':'cc',
			'alipay_trade_amt':56,
			'alipay_trade_num':4
		}
	];

    test.deepEqual(sp.load(),exp,'sum插件 有group by 是错误');

	test.done();
}
