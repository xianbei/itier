//var Select = require("../../core/parse/select");

var Cfg3 = require("../../src/hotcfg").Cfg3;
var SqlParser = require("../../src/source/sqlParser").ctor;
var SqlCache = require("../../src/sqlcache");
var cfg = new Cfg3("../../actions", "ActionLoader");

setTimeout(function(){

  benchTest();

}, 2000);

function inspect(obj){
  console.log(require("util").inspect(obj ,true ,10));
}

function startBench(){
  start = +new Date;
}

function endBench(){
  end = +new Date;
  console.log("time :"+ (end -start));
}

var count = 100000;

function benchTest(){
  startBench();
  var rObj;
  var sObj;
  for(var i=0;i< count; i++){
    rObj = SqlCache.get("/app/sub1/mysql/action1/where/id:eq:1");
    sObj = rObj[0];
    console.log(i + "ok");
  }
  endBench();
}
