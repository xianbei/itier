// +------------------------------------------------------------------------+
// | core/parse/lexter.js单元测试                                           |
// +------------------------------------------------------------------------+
// | Copygight (c) 2003 - 2011 Taobao.com. All Rights Reserved              |
// +------------------------------------------------------------------------+
// | Author: yixuan.zzq <yixuan.zzq@taobao.com>                             |
// +------------------------------------------------------------------------+
// | 2011年10月                                                             |
// +------------------------------------------------------------------------+

var Cases	= require('nodeunit').testCase;
var Lexter	= require('../../core/parse/lexter.js');

module.exports	= Cases({
	setUp	: function(callback) {
		callback();
	},

	tearDown : function(callback){
		callback();
	},

	/* {{{ test_should_simple_select_with_comment_parse_ok() */
	test_should_simple_select_with_comment_parse_ok	: function(test) {
		var lexter	= Lexter.create(
			"SELECT /** 我是注释 **/ 123, `password`, MD5(\"123456\") FROM mysql.user WHERE user=\"测试\\\"引号\" AND host!='%'"
		);

		test.deepEqual([
			{'text'	: 'SELECT', 'type' : Lexter.types.KEYWORD},
			{'text'	: '我是注释', 'type' : Lexter.types.COMMENT},
			{'text'	: 123, 'type' : Lexter.types.NUMBER},
			{'text'	: ',', 'type' : Lexter.types.COMMAS},
			{'text'	: 'password', 'type' : Lexter.types.VARIABLE},
			{'text'	: ',', 'type' : Lexter.types.COMMAS},
			{'text'	: 'MD5', 'type' : Lexter.types.FUNCTION},
			{'text'	: '(', 'type' : Lexter.types.COMMAS},
			{'text'	: '123456', 'type' : Lexter.types.STRING},
			{'text'	: ')', 'type' : Lexter.types.COMMAS},
			{'text'	: 'FROM', 'type' : Lexter.types.KEYWORD},
			{'text'	: 'mysql.user', 'type' : Lexter.types.KEYWORD},
			{'text'	: 'WHERE', 'type' : Lexter.types.KEYWORD},
			{'text'	: 'user', 'type' : Lexter.types.KEYWORD},
			{'text'	: '=', 'type' : Lexter.types.OPERATOR},
			{'text'	: '测试"引号', 'type' : Lexter.types.STRING},
			{'text'	: 'AND', 'type' : Lexter.types.KEYWORD},
			{'text'	: 'host', 'type' : Lexter.types.KEYWORD},
			{'text'	: '!=', 'type' : Lexter.types.OPERATOR},
			{'text'	: '%', 'type' : Lexter.types.STRING},
		], lexter.getAll());

		test.done();
	},
	/* }}} */

  /* {{{ test_should_negative_number_be_parsed_ok() */
  test_should_negative_number_be_parsed_ok    : function(test) {
    var lexter  = Lexter.create('SELECT a, c-1 FROM table WHERE b=-2');
    test.deepEqual([
      {text : 'SELECT', type : Lexter.types.KEYWORD},
      {text : 'a', type : Lexter.types.KEYWORD},
      {text : ',', type : Lexter.types.COMMAS},
      {text : 'c', type : Lexter.types.KEYWORD},
      {text : -1, type : Lexter.types.NUMBER},
      {text : 'FROM', type : Lexter.types.KEYWORD},
      {text : 'table', type : Lexter.types.KEYWORD},
      {text : 'WHERE', type : Lexter.types.KEYWORD},
      {text : 'b', type : Lexter.types.KEYWORD},
      {text : '=', type : Lexter.types.OPERATOR},
      {text : -2, type : Lexter.types.NUMBER},
    ], lexter.getAll());
    test.done();
  },
  /* }}} */

  /* {{{ test_should_parse_bind_variable_works_fine() */
  test_should_parse_bind_variable_works_fine  : function(test) {
    var lexter  = Lexter.create('c=:id AND t=:V_1');
    test.deepEqual([
      {text : 'c', type : Lexter.types.KEYWORD},
      {text : '=', type : Lexter.types.OPERATOR},
      {text : ':id', type : Lexter.types.PARAMS},
      {text : 'AND', type : Lexter.types.KEYWORD},
      {text : 't', type : Lexter.types.KEYWORD},
      {text : '=', type : Lexter.types.OPERATOR},
      {text : ':V_1', type : Lexter.types.PARAMS},
    ], lexter.getAll());

    test.done();
  },
  /* }}} */

  /* {{{ test_should_number_be_parsed_ok() */
  /**
   * xxx: this is a bug case
   */
  test_should_number_be_parsed_ok : function(test) {
    var lexter  = Lexter.create(123402);
    test.deepEqual([
      {text : 123402, type : Lexter.types.NUMBER}
    ], lexter.getAll());
    test.done();
  },
  /* }}} */

  /* {{{ test_should_expression_parsed_ok() */
  /**
   * xxx: this is a bug case for /ABS
   */
  test_should_expression_parsed_ok    : function(test) {
    var lexter  = Lexter.create('1+2*3.783/ABS(-3.6)');
    test.deepEqual([
      {text : 1, type : Lexter.types.NUMBER},
      {text : '+', type : Lexter.types.OPERATOR},
      {text : 2, type : Lexter.types.NUMBER},
      {text : '*', type : Lexter.types.OPERATOR},
      {text : 3.783, type : Lexter.types.NUMBER},
      {text : '/', type : Lexter.types.OPERATOR},
      {text : 'ABS', type : Lexter.types.FUNCTION},
      {text : '(', type : Lexter.types.COMMAS},
      {text : -3.6, type : Lexter.types.NUMBER},
      {text : ')', type : Lexter.types.COMMAS},
    ], lexter.getAll());

    test.done();
  },
  /* }}} */

  /* {{{ test_should_token_indexof_works_fine() */
  test_should_token_indexof_works_fine    : function(test) {
    var lexter  = Lexter.create('1+2*3.783/ABS(x + 3.6) + y');
    var commas  = {
      type    : Lexter.types.OPERATOR,
      text    : '+'
    };

    test.equal( 1, lexter.indexOf(commas));
    test.equal(12, lexter.indexOf(commas, 1));

    // xxx: x 后边的+号不应该被识别出
    test.equal(12, lexter.indexOf(commas, 8));
    test.equal(-1, lexter.indexOf(commas, 12));

    // xxx: 正则表达式匹配
    test.equal(6, lexter.indexOf({
      type    : Lexter.types.FUNCTION,
      text    : 'a',
    }));

    test.equal(-1, lexter.indexOf({
      type    : Lexter.types.FUNCTION,
      text    : '^a$',
    }));

    test.done();
  },
  /* }}} */

/*{{{ test_should_get_operator_vars_works_fine()*/	
	test_should_get_operator_vars_works_fine : function(test){
		var lexter = Lexter.create("-1+v+FUNCTION(a,FUNC2(b))");
		var expect = [
			{text:-1,type:Lexter.types.NUMBER},
			{text:"v",type:Lexter.types.KEYWORD}
		];
		test.deepEqual(Lexter.vars(1,"-1+v+FUNCTION(a,FUNC2(b))",true),expect);
		expect = [
			[{text:"a",type:Lexter.types.KEYWORD}],
			[{text:"FUNC2",type:Lexter.types.FUNCTION},
			 {text:"(",type:Lexter.types.COMMAS},
			 {text:"b",type:Lexter.types.KEYWORD},
			 {text:")",type:Lexter.types.COMMAS}]
		];
		test.deepEqual(Lexter.vars(4,lexter.getAll(),false),expect);
		expect = [
			[{text:"b",type:Lexter.types.KEYWORD}]
		];
		test.deepEqual(Lexter.vars(8,lexter.getAll(),false),expect);
		test.done();
	},
/*}}}*/

/*{{{ test_should_get_text_works_fine()*/
	test_should_get_text_works_fine : function(test){
		var t = [
			{type:Lexter.types.FUNCTION,text:"ABS"},
			{type:Lexter.types.COMMAS,text:"("},
			{type:Lexter.types.NUMBER,text:-3.6},
			{type:Lexter.types.STRING,text:")"}
		];
		var expect = Lexter.text(t," OR ").join("");
		test.deepEqual("ABS OR ( OR -3.6 OR \')\'",expect);
		test.done();
	}
/*}}}*/

});

