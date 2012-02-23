// +------------------------------------------------------------------------+
// | core/parse/select.js单元测试                                           |
// +------------------------------------------------------------------------+
// | Copygight (c) 2003 - 2011 Taobao.com. All Rights Reserved              |
// +------------------------------------------------------------------------+
// | Author: yixuan.zzq <yixuan.zzq@taobao.com>                             |
// +------------------------------------------------------------------------+
// | 2011年10月                                                             |
// +------------------------------------------------------------------------+

var Cases = require('nodeunit').testCase;
var Select = require('../../core/parse/select.js');
var Lexter = require('../../core/parse/lexter.js');
var util = require("util");

module.exports = Cases({
	setUp: function(callback) {
		callback();
	},

	tearDown: function(callback) {
		callback();
	},

	/* {{{ test_should_throw_exception_when_does_not_begin_with_select or parenthese() */
	test_should_throw_exception_when_does_not_begin_with_select: function(test) {
		try {
			var select = Select.create("`SELECT` 1 as b");
			select.get();
			test.ok(false);
		} catch(err) {
			test.ok(err instanceof Error);
			test.equal("SQL command should begin with keyword *SELECT*", err.message);
		}
		test.done();
	},
	/* }}} */

  /*{{{ test_should_parse_right_union_works_fine()*/
	test_should_parse_right_union_works_fine: function(test) {
    var sql = Select.create('SELECT a FROM b uNiOn (SELECT m FROM n UNION SELECT T FROM V)');
    var eql = [
      [
        {'text':'SELECT','type':Lexter.types.KEYWORD},
        {'text':'a','type':Lexter.types.KEYWORD},
        {'text':'FROM','type':Lexter.types.KEYWORD},
        {'text':'b','type':Lexter.types.KEYWORD},
      ],
      [
        {'text':'SELECT','type':Lexter.types.KEYWORD},
        {'text':'m','type':Lexter.types.KEYWORD},
        {'text':'FROM','type':Lexter.types.KEYWORD},
        {'text':'n','type':Lexter.types.KEYWORD},
        {'text':'UNION','type':Lexter.types.KEYWORD},
        {'text':'SELECT','type':Lexter.types.KEYWORD},
        {'text':'T','type':Lexter.types.KEYWORD},
        {'text':'FROM','type':Lexter.types.KEYWORD},
        {'text':'V','type':Lexter.types.KEYWORD}
      ]
    ];
    test.deepEqual(eql,sql.ins.union());
    test.done();
  },
  /*}}}*/

	/* {{{ test_should_parse_right_select_columns() */
	test_should_parse_right_select_columns: function(test) {
		var select = Select.create('sElECt a, 1 b, 1+MD5("123") AS `select`, MAX(d), DistInct user.username');
		//var select = Select.create("SELECT DISTINCT(model_id) AS id,model_name as name FROM dim_category_brand_prd WHERE category_level2 = 50011980 AND brand_id = 407961    7 AND deleted = 0 AND model_name <> '' ORDER BY model_name ASC");
		var eq = [];
		eq['a'] = {
			dist: null,
			expr: [{
				text: 'a',
				type: 1
			}]
		};
		eq['b'] = {
			dist: null,
			expr: [{
				text: 1,
				type: 2
			}]
		};
		eq['select'] = {
			dist: null,
			expr: [{
				text: 1,
				type: 2
			},
			{
				text: '+',
				type: 7
			},
			{
				text: 'MD5',
				type: 4
			},
			{
				text: '(',
				type: 8
			},
			{
				text: '123',
				type: 3
			},
			{
				text: ')',
				type: 8
			}]
		};
		eq['MAX(d)'] = {
			dist: null,
			expr: [{
				text: 'MAX',
				type: 4
			},
			{
				text: '(',
				type: 8
			},
			{
				text: 'd',
				type: 1
			},
			{
				text: ')',
				type: 8
			}]
		};
		eq['username'] = {
			dist: {
				text: 'DistInct',
				type: 1
			},
			expr: [{
				text: 'user.username',
				type: 1
			}]
		};
		test.deepEqual(
		eq, select.get()[0].columns);
		test.done();
	},
	/* }}} */

	/*{{{test_should_parse_right_source_works_fine()*/
	test_should_parse_right_source_works_fine: function(test) {
		var sql = Select.create('SELECT t FROM mysql.tableA As a, tableB b, c, action.ActionA as d, (select s FROM s) as e, (select u From u UNION select v FROM v) aS f WHERE w ORDER BY bbbb x');
        var eql = {};
        eql.a = {
            type:"table",
            db:"mysql",
            source:"tableA"
        };
        eql.b = {
            type:"table",
            db:"",
            source:"tableB"
        };
        eql.c = {
            type:"table",
            db:"",
            source:"c"
        };
        eql.d = {
            type:"action",
            source:"ActionA"
        };
        eql.e = {
            type:"sql",
            source:" select s FROM s "
        };
        eql.f = {
            type:"sql",
            source:" select u From u UNION select v FROM v "
        };
		test.deepEqual(
		eql, sql.get()[0].sources);
		test.done();
	},
	/*}}}*/

	/*{{{test_should_parse_right_join_works_fine() */
	test_should_parse_right_join_works_fine: function(test) {
		var sql = Select.create('SELECT a.c1, m.c2 FROM a LEFT JOIN action.Action1 as m ON a.c3=m.c3 AND a.c4=b.c4 JOIN db.tab_c as c ON c.id=a.c2 RIGHT JOIN (SELECT m FROM M) as s ON s.id=a.id');
    var res = sql.get();
		test.deepEqual({
      type:"action",
      source:"Action1",
			method: 'LEFT JOIN',
			where: 'a.c3 = m.c3 AND a.c4 = b.c4'
		},
		res[0].joinmap.m);
		test.deepEqual({
      type:"table",
      db:"db",
			source: 'tab_c',
			method: 'JOIN',
			where: 'c.id = a.c2'
		},
		res[0].joinmap.c);
    test.deepEqual({
      type:"sql",
      source:" SELECT m FROM M ",
      method:"RIGHT JOIN",
      where:"s.id = a.id"
    },
    res[0].joinmap.s);
		test.done();
	},
	/*}}}*/

  /*{{{ test_should_parse_right_no_alias_source_and_join_works_fine()*/
  test_should_parse_right_no_alias_source_and_join_works_fine : function(test){
  var sql = Select.create('SELECT a FROM mysql.tab LEFT JOIN action.Action ON mysql.tab.id=action.Action.id');
    var res = sql.get();
    var eql = {};
    eql["mysql.tab"] = {
      type:"table",
      db:"mysql",
      source:"tab"
    }
    test.deepEqual(eql,res[0].sources);
    eql = {};
    eql.type = "action";
    eql.source = "Action";
    eql.method = "LEFT JOIN";
    eql.where = "mysql.tab.id = action.Action.id";
    test.deepEqual(eql,res[0].joinmap["action.Action"]);
    test.done();
  },
  /*}}}*/

	/*{{{ test_should_parse_right_where_works_fine() */
	test_should_parse_right_where_works_fine: function(test) {
		var sql = Select.create('SelEcT * FROM table WHERE a=b AND c >= "id" AND thedate BETWEEN (100 AND 200) AND t IN (2,5,"6") and m LIKE "%abc%" AND 1 <> 2 AND d is not null AND p NOT LIKE "8" AND db.table.x NOT IN (2) AND z is null');
        /*
		var sql = Select.create('SelEcT * FROM table WHERE a=1 AND ##WHERE##');
        var eql = { 
            columns: { '*': { dist: null, expr: [ { text: '*', type: 7 } ] } },
            sources: { table: { type: 'table', db: '', source: 'table' } },
            joinmap: {},
            where: 'a = 1 AND \'##WHERE##\'',
            groupby: [],
            orderby: [],
            limits: '##LIMIT##' };
        test.deepEqual(eql,sql.get()[0]);
        test.done();
        */
        /*
        var where = 'a=b AND c >= "id" AND thedate BETWEEN (100 AND 200) AND t IN (2,5,"6") and m LIKE "%abc%" AND 1 <> 2 AND d is not null AND p NOT LIKE "8" AND db.table.x NOT IN (2) AND z is null';
        var parse = Select.create("");
        var result = parse.parseWhere(where);
        */
		test.deepEqual([{
			relate: Select.WHERE.EQ,
			values: [{
				text: 'b',
				type: 1
			}],
			column: {
				text: 'a',
				type: 1
			}
		},

		{
			relate: Select.WHERE.GE,
			values: [{
				text: 'id',
				type: 3
			}],
			column: {
				text: 'c',
				type: 1
			}
		},

		{
			relate: Select.WHERE.BETWEEN,
			values: [{
				text: 100,
				type: 2
			},
			{
				text: 200,
				type: 2
			}],
			column: {
				text: 'thedate',
				type: 1
			}
		},

		{
			relate: Select.WHERE.IN,
			values: [{
				text: 2,
				type: 2
			},
			{
				text: 5,
				type: 2
			},
			{
				text: '6',
				type: 3
			}],
			column: {
				text: 't',
				type: 1
			}
		},

		{
			relate: Select.WHERE.LIKE,
			values: [{
				text: '%abc%',
				type: 3
			}],
			column: {
				text: 'm',
				type: 1
			}
		},

		{
			relate: Select.WHERE.NE,
			values: [{
				text: 2,
				type: 2
			}],
			column: {
				text: 1,
				type: 2
			}
		},

		{
			relate: Select.WHERE.NOTNULL,
			values: null,
			column: {
				text: 'd',
				type: 1
			}
		},

		{
			relate: Select.WHERE.NOTLIKE,
			values: [{
				text: '8',
				type: 3
			}],
			column: {
				text: 'p',
				type: 1
			}
		},

		{
			relate: Select.WHERE.NOTIN,
			values: [{
				text: 2,
				type: 2
			}],
			column: {
				text: 'db.table.x',
				type: 1
			}
		},

		{
			relate: Select.WHERE.ISNULL,
			values: null,
			column: {
				text: 'z',
				type: 1
			}
		}], sql.get()[0].where);
		test.done();
	},
	/*}}}*/

	/*{{{test_should_parse_right_groupby_fine()*/
	test_should_parse_right_groupby_fine: function(test) {
		var sql = Select.create('SELECT * FROM table gRouP by c, CONCAT(`status`, "wo")');
		test.deepEqual([[{
			text: 'c',
			type: 1
		}], [{
			text: 'CONCAT',
			type: 4
		},
		{
			text: '(',
			type: 8
		},
		{
			text: 'status',
			type: 5
		},
		{
			text: ',',
			type: 8
		},
		{
			text: 'wo',
			type: 3
		},
		{
			text: ')',
			type: 8
		}]], sql.get()[0].groupby);
		test.done();
	},
	/*}}}*/

	/*{{{test_should_parse_right_orderby_works_fine() */
	test_should_parse_right_orderby_works_fine: function(test) {
		var sql = Select.create('SELECT * FROM tab ORDER BY a DESC, MD5(b), c ASC');
		test.deepEqual([{
			type: 2,
			expr: [{
				text: 'a',
				type: 1
			}]
		},
		{
			type: 1,
			expr: [{
				text: 'MD5',
				type: 4
			},
			{
				text: '(',
				type: 8
			},
			{
				text: 'b',
				type: 1
			},
			{
				text: ')',
				type: 8
			}]
		},
		{
			type: 1,
			expr: [{
				text: 'c',
				type: 1
			}]
		}

		], sql.get()[0].orderby);
		test.done();
	},
	/*}}}*/

	/*{{{test_should_parse_right_limit_works_fine() */
	test_should_parse_right_limit_works_fine: function(test) {
		var sql = Select.create('Select * from asldf LIMIT 10');
      var parse = Select.create("");
      var result = parse.parseLimit("10");
		test.deepEqual([{
			text: 0,
			type: 2
		},
		{
			text: 10,
			type: 2
		}], result);
		test.done();
	},
	/*}}}*/

	/*{{{test_should_parse_right_groupby_fine()*/
	test_should_parse_right_groupby_fine888: function(test) {
		var sql = Select.create('SELECT brand_id,SUM(collector_num) from rpt_brand_buyer_area_d where thedate = "2010-10-10" and brand_id = "10122" and category_id = "110520" limit 6');
    var res = '';
    var result = sql.get();
    for(var tk in result[0]){
        res += tk + ',';
    }
    res = res.substr(0,res.length-2);
    test.deepEqual('columns,sources,joinmap,where,groupby,orderby,limit',res);
		test.done();
	},
	/*}}}*/

});

