// +------------------------------------------------------------------------+
// | src/parse/url.js单元测试                                               |
// +------------------------------------------------------------------------+
// | Copygight (c) 2003 - 2011 Taobao.com. All Rights Reserved              |
// +------------------------------------------------------------------------+
// | Author: yixuan.zzq <yixuan.zzq@taobao.com>                             |
// +------------------------------------------------------------------------+
// | 2011年10月                                                             |
// +------------------------------------------------------------------------+

var Cases = require('nodeunit').testCase;
var UrlParser = require("../../src/parse/url.js");

module.exports = Cases({
	setUp: function(callback) {
		callback();
	},
	tearDown: function(callback) {
		callback();
	},

    /*{{{ test_should_condition_right_work_fine()*/
    test_should_parse_condition_right_work_fine : function(test){
        var urlParser = UrlParser.create("/db/keywords/fact_query_effect_d/get_title_brief/where/f0:eq:2011-10-16/where/r1:ge:2011-10-10/where/r1:le:2011-10-16/where/keytype:eq:title/where/query:eq:QUERY/where/aid:eq:4458633437/where/utype:eq:0/where/nick:eq:cubetest04/where/uid:eq:233205/limit/3,5");
        var eql = {
            app: "db", 
            modules: "keywords/fact_query_effect_d",
            action: "get_title_brief",
            where: {
                "f0=":"2011-10-16",
                "r1>=":"2011-10-10",
                "r1<=":"2011-10-16",
                "keytype=":"title",
                "query=":"QUERY",
                "aid=":"4458633437",
                "utype=":"0",
                "nick=":"cubetest04",
                "uid=":"233205"
            },
            limit:[3,5]
        };
        test.deepEqual(eql,urlParser.parse());
        test.done();
    },
    /*}}}*/

    /*{{{ test_should_catch_exception_work_fine()*/
    test_should_catch_exception_work_fine : function(test){
        var urlParser = UrlParser.create("/db/keywords/fact_query_effect_d/get_title_brief/where/f0:qq:2011-10-16/where/r1:ge:2011-10-10");
        try{
            urlParser.parse();
            test.ok(false,"url wrong");
        }catch(e){
            if(e.message == "wrong operator"){
                test.ok(true,"every thing is ok");
            }else{
                test.ok(false,"exception");
            }
        }
        test.done();
    },
    /*}}}*/

});
