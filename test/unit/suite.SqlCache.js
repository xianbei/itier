var Cases = require('nodeunit').testCase;
var SqlCache = require("../../src/sqlcache.js");
var util = require("util");

module.exports = Cases({
	setUp: function(callback) {
		callback();
	},

	tearDown: function(callback) {
		callback();
	},

  /*{{{ test_a_normal_case_with_where_replace_token()*/
  test_a_normal_case_with_where_replace_token : function(test){
    var config = {
      path:"/a/b/c",
      actions:{
        action1:{
          config:"select a,b from table where ##WHERE## LIMIT ##LIMIT##",
          params:{
            "name":[{
                type:"string",
                replace:"wangwang",
                pos:"##WHERE##"
            }]
          }
        }
      }
    }
    SqlCache.set(config);
    var eql = 
      [ { columns:
          { a: { dist: null, expr: [ { text: 'a', type: 1 } ] },
            b: { dist: null, expr: [ { text: 'b', type: 1 } ] } },
          sources: { table: { type: 'table', db: '', source: 'table' } },
          joinmap: {},
          where:
            [ { relate: 1,
                values: [ { text: '中国风', type: 3 } ],
                column: { text: 'wangwang', type: 1 } } ],
          groupby: [],
          orderby: [],
          limits: [ { text: 0, type: 2 }, { text: 1, type: 2 } ] } 
      ];
    test.deepEqual(eql,SqlCache.get("/a/b/c/action1/where/name:eq:中国风/limit/1"));
    test.done();
  },
  /*}}}*/

  /*{{{ test_a_normal_case_with_keyword_in_in_where()*/
  test_a_normal_case_with_keyword_in_in_where : function(test){
    var config = {
      path:"/a/b/p",
      actions:{
        action1:{
          config:"select a,b from table where ##WHERE## LIMIT ##LIMIT##",
          params:{
            "name":[{
                type:"string",
                replace:"wangwang",
                pos:"##WHERE##"
            }]
          }
        }
      }
    }
    SqlCache.set(config);
    var eql = 
      [ { columns:
          { a: { dist: null, expr: [ { text: 'a', type: 1 } ] },
            b: { dist: null, expr: [ { text: 'b', type: 1 } ] } },
          sources: { table: { type: 'table', db: '', source: 'table' } },
          joinmap: {},
          where:
              [ { relate: 7,
                  values: [ { text: '中国风', type: 3 },{ text: '美国风', type: 3 } ],
                  column: { text: 'wangwang', type: 1 } } ],
          groupby: [],
          orderby: [],
          limits: [ { text: 0, type: 2 }, { text: 1, type: 2 } ] } ];
    test.deepEqual(eql,SqlCache.get("/a/b/p/action1/where/name:in:中国风,美国风/limit/1"));
    test.done();
  },
  /*}}}*/

  /*{{{ test_where_with_two_different_tokens()*/
  test_where_with_two_different_tokens : function(test){
    var config = {
      path:"/a/b/d",
      actions:{
        action1:{
          config:"select a,b from table where ##WHERE## AND ##WHERE1## LIMIT ##LIMIT##",
          params:{
            "name":[{
                type:"string",
                replace:"wangwang",
                pos:"##WHERE##"
            }],
            "name2":[{
                type:"string",
                replace:"wangwang2",
                pos:"##WHERE1##"
            }]
          }
        }
      }
    }
    SqlCache.set(config);
    var eql = 
      [ { columns:
          { a: { dist: null, expr: [ { text: 'a', type: 1 } ] },
            b: { dist: null, expr: [ { text: 'b', type: 1 } ] } },
          sources: { table: { type: 'table', db: '', source: 'table' } },
          joinmap: {},
          where:
              [ { relate: 1,
                  values: [ { text: 'yixuan.zzq', type: 3 } ],
                  column: { text: 'wangwang', type: 1 } },
          { relate: 1,
            values: [ { text: 'xuyi.zl', type: 3 } ],
            column: { text: 'wangwang2', type: 1 } } ],
          groupby: [],
          orderby: [],
          limits: []}]
    test.deepEqual(eql,SqlCache.get('/a/b/d/action1/where/name:eq:yixuan.zzq/where/name2:eq:xuyi.zl'));
    test.done();
  },
  /*}}}*/

  /*{{{ test_inner_sql_complated_works_fine()*/
  test_inner_sql_complated_works_fine : function(test){
    var config = {
      path:"/a/b/e",
      actions:{
        action1:{
          config:"select a,b from (select f,g from table where m=1 AND ##WHERE1## AND n=2) as t, (select k,j from table where ##WHERE1##) as t1,table2 as t2, table3 RIGHT JOIN (SELECT j from tab where a = b) as table4 ON j.m=1 where ##WHERE2## AND k=2 LIMIT ##LIMIT1##",
          params:{
            "f0":[{
                type:"int",
                replace:"f00",
                pos:"##WHERE1##"
            }],
            "f1":[{
                type:"int",
                replace:"",
                pos:"##WHERE2##"
            }],
            "f2":[{
                type:"date",
                replace:"",
                pos:"##WHERE1##"
            }],
          }
        }
      }
    }
    SqlCache.set(config);
    var eql = 
    [ { columns:
        { a: { dist: null, expr: [ { text: 'a', type: 1 } ] },
          b: { dist: null, expr: [ { text: 'b', type: 1 } ] } },
        sources:
        { t:
          { type: 'sql',
            source:
            [ { columns:
                { f: { dist: null, expr: [ { text: 'f', type: 1 } ] },
                  g: { dist: null, expr: [ { text: 'g', type: 1 } ] } },
                sources: { table: { type: 'table', db: '', source: 'table' } },
                joinmap: {},
                where:
                  [ { relate: 1,
                      values: [ { text: 1, type: 2 } ],
                      column: { text: 'm', type: 1 } },
                    { relate: 1,
                      values: [ { text: 2, type: 2 } ],
                      column: { text: 'n', type: 1 } },
                    { relate: 1,
                      values: [ { text: '12', type: 2 } ],
                      column: { text: 'f00', type: 1 } },
                    { relate: 1,
                      values: [ { text: '2011-11-11', type: 3 } ],
                      column: { text: 'f2', type: 1 } } ],
                groupby: [],
                orderby: [],
                limits: [] } ] },
          t1:
            { type: 'sql',
              source:
                [ { columns:
                    { k: { dist: null, expr: [ { text: 'k', type: 1 } ] },
                      j: { dist: null, expr: [ { text: 'j', type: 1 } ] } },
                    sources: { table: { type: 'table', db: '', source: 'table' } },
                    joinmap: {},
                    where:
                      [{ relate: 1,
                         values: [ { text: '12', type: 2 } ],
                         column: { text: 'f00', type: 1 } },
                       { relate: 1,
                         values: [ { text: '2011-11-11', type: 3 } ],
                         column: { text: 'f2', type: 1 } } ],
                    groupby: [],
                    orderby: [],
                    limits: [] } ] },
          t2: { type: 'table', db: '', source: 'table2' },
          table3: { type: 'table', db: '', source: 'table3' } },
        joinmap:
          { table4:
            { type: 'sql',
              source:
                [ { columns: { j: { dist: null, expr: [ { text: 'j', type: 1 } ] } },
                    sources: { tab: { type: 'table', db: '', source: 'tab' } },
                    joinmap: {},
                    where:
                      [ { relate: 1,
                          values: [ { text: 'b', type: 1 } ],
                          column: { text: 'a', type: 1 } } ],
                          groupby: [],
                          orderby: [],
                          limits: [] } ],
              method: 'RIGHT JOIN',
              where: 'j.m = 1' } },
         where:
            [ { relate: 1,
                values: [ { text: 2, type: 2 } ],
                column: { text: 'k', type: 1 } },
              { relate: 1,
                values: [ { text: '14', type: 2 } ],
                column: { text: 'f1', type: 1 } } ],
                groupby: [],
                orderby: [],
         limits: [ { text: 3, type: 2 }, { text: 5, type: 2 } ] } ];
    test.deepEqual(eql,SqlCache.get("/a/b/e/action1/where/f0:eq:12/where/f1:eq:14/where/f2:eq:2011-11-11/where/f4:eq:15/limit/3,5"));
    test.done();
  },
  /*}}}*/

  /*{{{ test_for_union_case()*/
  test_for_union_case : function(test){
    var config = {
      path:"/a/b/f",
      actions:{
        "test_union":{
          "config":"SELECT b.category_id AS f0,b.category_name AS f00,SUM(a.alipay_trade_amt) AS f1 FROM myfox.rpt_cat_info_d AS a INNER JOIN myfox.dim_category AS b ON a.category_id=b.category_id WHERE ##WHERE1## AND ##WHERE## GROUP BY f0,f00 ORDER BY f1 DESC LIMIT 5 union SELECT b.category_id AS f0,b.category_name AS f00,SUM(a.alipay_trade_amt) AS f1 FROM myfox.rpt_cat_info_d AS a INNER JOIN myfox.dim_category AS b ON a.category_id=b.category_id WHERE ##WHERE2## AND ##WHERE## GROUP BY f0,f00 ORDER BY f1 DESC LIMIT 5",
          "params":{
            "cid1":[{
                "type":"int",
                "pos":"##WHERE1##",
                "replace":"a.category_id"
            }],
            "cid2":[{
                "type":"int",
                "pos":"##WHERE2##",
                "replace":"a.category_id"
            }],
            "r1":[{
                "type":"date",
                "pos":"##WHERE##",
                "replace":"a.thedate"
            }]
          }
        }
      }
    }
    SqlCache.set(config);
    var eql = 
      [ { columns:
          { f0:
              { dist: null,
                expr: [ { text: 'b.category_id', type: 1 } ] },
            f00:
              { dist: null,
                expr: [ { text: 'b.category_name', type: 1 } ] },
            f1:
              { dist: null,
                expr:
                  [ { text: 'SUM', type: 4 },
                  { text: '(', type: 8 },
                  { text: 'a.alipay_trade_amt', type: 1 },
                  { text: ')', type: 8 } ] } },
          sources:
           { a:
              { type: 'table',
                source: 'rpt_cat_info_d',
                db: 'myfox' } },
          joinmap:
          { b:
              { type: 'table',
                source: 'dim_category',
                db: 'myfox',
                method: 'INNER JOIN',
                where: 'a.category_id = b.category_id' } },
          where:
            [ { relate: 1,
                values: [ { text: '1101', type: 2 } ],
                column: { text: 'a.category_id', type: 1 } },
              { relate: 3,
                values: [ { text: '2010-10-10', type: 3 } ],
                column: { text: 'a.thedate', type: 1 } },
              { relate: 5,
                values: [ { text: '2010-10-10', type: 3 } ],
                column: { text: 'a.thedate', type: 1 } } ],
          groupby:
              [ [ { text: 'f0', type: 1 } ],
                [ { text: 'f00', type: 1 } ] ],
          orderby: [ { type: 2, expr: [ { text: 'f1', type: 1 } ] } ],
          limits: [ { text: 0, type: 2 }, { text: 5, type: 2 } ] },
      { columns:
          { f0:
              { dist: null,
                expr: [ { text: 'b.category_id', type: 1 } ] },
          f00:
              { dist: null,
                expr: [ { text: 'b.category_name', type: 1 } ] },
          f1:
              { dist: null,
                expr:
                  [ { text: 'SUM', type: 4 },
                  { text: '(', type: 8 },
                  { text: 'a.alipay_trade_amt', type: 1 },
                  { text: ')', type: 8 } ] } },
          sources:
           { a:
              { type: 'table',
                source: 'rpt_cat_info_d',
                db: 'myfox' } },
          joinmap:
            { b:
              { type: 'table',
                source: 'dim_category',
                db: 'myfox',
                method: 'INNER JOIN',
                where: 'a.category_id = b.category_id' } },
          where:
           [ { relate: 1,
               values: [ { text: '1512', type: 2 } ],
               column: { text: 'a.category_id', type: 1 } },
             { relate: 3,
               values: [ { text: '2010-10-10', type: 3 } ],
               column: { text: 'a.thedate', type: 1 } },
             { relate: 5,
               values: [ { text: '2010-10-10', type: 3 } ],
               column: { text: 'a.thedate', type: 1 } } ],
          groupby:
           [ [ { text: 'f0', type: 1 } ],
             [ { text: 'f00', type: 1 } ] ],
          orderby: [ { type: 2, expr: [ { text: 'f1', type: 1 } ] } ],
          limits: [ { text: 0, type: 2 }, { text: 5, type: 2 } ] } ]

    test.deepEqual(eql,SqlCache.get('/a/b/f/test_union/where/cid1:eq:1101/where/cid2:eq:1512/where/r1:ge:2010-10-10/where/r1:le:2010-10-10/limit/5'));
    test.done();
  },
  /*}}}*/

  /*{{{ test_for_not_enough_params_in_url */
  test_for_not_enough_params_in_url : function(test){
    var config = {
      path:"/a/b/c",
      actions:{
        action1:{
          config:"select a,b from table where ##WHERE## LIMIT ##LIMIT##",
          params:{
            "name":[{
                type:"string",
                replace:"wangwang",
                pos:"##WHERE##"
            }],
            "name2":[{
                type:"string",
                replace:"",
                pos:"WHERE"
            }]
          }
        }
      }
    }
    var expect = [ 
      { columns:
          { a: { dist: null, expr: [ { text: 'a', type: 1 } ] },
            b: { dist: null, expr: [ { text: 'b', type: 1 } ] } },
        sources: { table: { type: 'table', db: '', source: 'table' } },
        joinmap: {},
        where:
          [ { relate: 1,
              values: [ { text: 'yixuan.zzq', type: 3 } ],
              column: { text: 'wangwang', type: 1 } } ],
        groupby: [],
        orderby: [],
        limits: [ { text: 0, type: 2 }, { text: 5, type: 2 } ] } ];

    SqlCache.set(config);
    test.deepEqual(expect,SqlCache.get("/a/b/c/action1/where/name:eq:yixuan.zzq/limit/5"));
    test.done();
  },
  /*}}}*/

  /*{{{ test_for_wrong_param_and_then_not_enough_params_in_url()*/
  test_for_wrong_param_and_then_not_enough_params_in_url : function(test){
    var config = {
      path:"/a/b/c",
      actions:{
        action1:{
          config:"select a,b from table where ##WHERE## LIMIT ##LIMIT##",
          params:{
            "name":[{
                type:"string",
                replace:"wangwang",
                pos:"##WHERE##"
            }],
            "name2":[{
                type:"int",
                replace:"",
                pos:"WHERE"
            }]
          }
        }
      }
    }
    SqlCache.set(config);
    var get;
    try{
      get = SqlCache.get("/a/b/c/action1/where/name:eq:yixuan.zzq/where/name2:eq:xuyi.zl/limit/5");
    }catch(e){
      test.equal(e.message,"param cannot convert to \"int\" type");
      test.done();
    }
  },
  /*}}}*/

  /*{{{ test_for_param_replace_two_different_places()*/
  test_for_param_replace_two_different_places : function(test){
    var config = {
      path:"/a/b/m",
      actions:{
        action1:{
          config:"select a,b from table where ##WHERE## AND ##WHERE2## LIMIT ##LIMIT##",
          params:{
            "name":[{
                type:"string",
                replace:"wangwang",
                pos:"##WHERE##"
            },{
                type:"string",
                replace:"wangwang2",
                pos:"##WHERE2##"
            }],
          }
        }
      }
    }
    SqlCache.set(config);
    var eql = 
      [ { columns:
          { a: { dist: null, expr: [ { text: 'a', type: 1 } ] },
            b: { dist: null, expr: [ { text: 'b', type: 1 } ] } },
          sources: { table: { type: 'table', db: '', source: 'table' } },
          joinmap: {},
          where:
            [ { relate: 1,
                values: [ { text: 'yixuan.zzq', type: 3 } ],
                column: { text: 'wangwang', type: 1 } },
              { relate: 1,
                values: [ { text: 'yixuan.zzq', type: 3 } ],
                column: { text: 'wangwang2', type: 1 } } ],
          groupby: [],
          orderby: [],
          limits: [ { text: 0, type: 2 }, { text: 5, type: 2 } ] } ]
    
    test.deepEqual(eql,SqlCache.get('/a/b/m/action1/where/name:eq:yixuan.zzq/limit/5'));
    test.done();
  },
  /*}}}*/

  /*{{{ test_for_inner_sql_in_where()*/
  test_for_inner_sql_in_where : function(test){
    var config = {
      path:"/a/b/o",
      actions:{
        "get_shop_hot_leafcat":{
          "config":"SELECT b.id AS id, b.level1Name AS level1Name, b.name AS name FROM ( SELECT d.category_id AS id, a.category_level1_name AS level1Name, a.category_name AS name, SUM(d.alipay_trade_amt) AS amt FROM myfox.rpt_clt_shop_mcat_d AS d INNER JOIN myfox.dim_category AS a ON m=n WHERE ##WHERE## AND d.leaf_flag=1 GROUP BY d.category_id ORDER BY amt DESC LIMIT 0,4) AS b",
          "params":{
            "sid":[{
              "type":"string",
              "pos":"##WHERE##",
              "replace":"d.shop_id"
            }],
            "r1":[{
              "type":"date",
              "pos":"##WHERE##",
              "replace":"d.thedate"
            }]
          }
        }
      }
    }
    SqlCache.set(config);

    var eql = 
      { b:
          { type: 'sql',
            source:
              [ { columns:
                  { id:
                      { dist: null,
                        expr: [ { text: 'd.category_id', type: 1 } ] },
                    level1Name:
                      { dist: null,
                        expr: [ { text: 'a.category_level1_name', type: 1 } ] },
                    name:
                      { dist: null,
                        expr: [ { text: 'a.category_name', type: 1 } ] },
                    amt:
                      { dist: null,
                        expr:
                          [ { text: 'SUM', type: 1 },
                          { text: '(', type: 8 },
                          { text: 'd.alipay_trade_amt', type: 1 },
                          { text: ')', type: 8 } ] } },
                  sources:
                   { d:
                      { type: 'table',
                        source: 'rpt_clt_shop_mcat_d',
                        db: 'myfox' } },
                  joinmap:
                  { a:
                      { type: 'table',
                        source: 'dim_category',
                        db: 'myfox',
                        method: 'INNER JOIN',
                        where: 'm = n' } },
                  where:
                      [ { relate: 1,
                          values: [ { text: 1, type: 2 } ],
                          column: { text: 'd.leaf_flag', type: 1 } },
                        { relate: 1,
                          values: [ { text: '279839', type: 3 } ],
                          column: { text: 'd.shop_id', type: 1 } },
                        { relate: 3,
                          values: [ { text: '2010-10-10', type: 3 } ],
                          column: { text: 'd.thedate', type: 1 } },
                        { relate: 5,
                          values: [ { text: '2010-10-10', type: 3 } ],
                          column: { text: 'd.thedate', type: 1 } } ],
                  groupby: [ [ { text: 'd.category_id', type: 1 } ] ],
                  orderby: [ { type: 2, expr: [ { text: 'amt', type: 1 } ] } ],
                  limits: [ { text: 0, type: 2 }, { text: 4, type: 2 } ] } 
              ] 
          }
      };

    test.deepEqual(eql,SqlCache.get('/a/b/o/get_shop_hot_leafcat/where/sid:eq:279839/where/r1:ge:2010-10-10/where/r1:le:2010-10-10')[0].sources);
    test.done();
  },
  /*}}}*/

  /*{{{ test_for_not_param_case()*/
  test_for_not_param_case : function(test){
    var config = {
      path:"/a/b/p",
      actions:{
        "get_test":{
          "config":"SELECT a.name FROM action.test_action AS a ORDER BY a.name DESC LIMIT ##LIMIT##"
        },
      }
    }
    SqlCache.set(config);

    var eql = 
      [ { columns: { name: { dist: null, expr: [ { text: 'a.name', type: 1 } ] } },
          sources: { a: { type: 'action', source: 'test_action' } },
          joinmap: {},
          where: [],
          groupby: [],
          orderby: [ { type: 2, expr: [ { text: 'a.name', type: 1 } ] } ],
          limits: [ { text: 0, type: 2 }, { text: 5, type: 2 } ] } ];

    test.deepEqual(eql,SqlCache.get("/a/b/p/get_test/where/cid:eq:16/limit/5"));
    test.done();
  },
  /*}}}*/

  /*{{{ test_for_param_bug_case()*/
  test_for_param_bug_case : function(test){
    var config = {
      path:"/taobaoindex/main/main",
      actions:{
        "get_lastday":{
          "config":"SELECT * FROM taobaoindex.lastday",
        }
      }
    }
    SqlCache.set(config);

    var eql = 
      [ { columns: { '*': { dist: null, expr: [ { text: '*', type: 7 } ] } },
      sources: { 'taobaoindex.lastday': { type: 'table', source: 'lastday', db: 'taobaoindex' } },
      joinmap: {},
      where: [],
      groupby: [],
      orderby: [],
      limits: [] } ];

    test.deepEqual(eql,SqlCache.get("/taobaoindex/main/main/get_lastday"));
    test.done();
  }
  /*}}}*/

});
