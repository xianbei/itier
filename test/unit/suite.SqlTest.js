
var Cases = require('nodeunit').testCase;
var Select = require('../../core/parse/select.js');
var Lexter = require('../../core/parse/lexter.js');
var Sql   = require('../../src/source/sql.js').ctor;
var util = require("util");

function inspect(obj){
  console.log(util.inspect(obj, false ,10));
}

module.exports = Cases({
	setUp: function(callback) {
		callback();
	},
	tearDown: function(callback) {
		callback();
	},
  test_all : function(test){
    var sqlStr = "select colName1 as c1, colName2 from hservice.table where id=1 AND type='hello' order by c1 desc limit 2 ";
    var sqlObj = Select.create(sqlStr).get()[0];

    //inspect(sqlObj);
    var sq = new Sql(sqlObj);
    var wheres = sq.getWheres();
    var columns = sq.getColNames();
    var asNames = sq.getAsNames();
    var orderby = sq.getOrderBy();
    var limits = sq.getLimits();


    test.deepEqual(wheres, {
      id : 1,
      type : 'hello'
    });
    
    test.deepEqual(columns, [
      'colName1',
      'colName2'
    ]);
    
    test.deepEqual(asNames, [
      'c1',
      'colName2'
    ]);

    test.deepEqual(orderby, [
      {
        name : 'c1',
        type : 'DESC'
      }
    ]);

    test.deepEqual(limits, [0, 2]);

    test.done();
  
  }
});
