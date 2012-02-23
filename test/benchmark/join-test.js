/**
 * @author : windyrobin@Gmail.com(Edward Zhang) 
 * @date:  12/31/2011 
 * 
 */

var Join = require("../../src/data/wjoin");
var leftJoin = Join.leftJoin;
var innerJoin = Join.innerJoin;


var on = {
  left : ["id", "pid"],
  right: ["id", "tid"]
  //left : ["id"],
  //right: ["id"]
};

var ta = {
  columns : ["id", "pid", "first"],
  data :[]
};

var tb = {
  columns : ["id", "tid", "last"],
  data :[]
};

function dataInit(){
  for(var i=0; i< 2000; i++){
    var a = [];
    var b = [];

    //var rid = parseInt(Math.random() * 5000);
    a.push(i);
    rid = parseInt(Math.random() * 10);
    a.push(rid);
    a.push("hello");

    b.push(i);
    rid = parseInt(Math.random() * 10);
    b.push(rid);
    b.push("world");

    ta.data.push(a);
    tb.data.push(b);
  }
}

var TEST_NUMBER = 10000;
function test(){
  dataInit();
  console.log("leftJoin test 2k X 2k...");
  var res;
  var start = new  Date();
  for(var i=0; i<TEST_NUMBER; i++){
    if(i % 2000 == 0) console.log(i);
    res = leftJoin(ta, tb, on);
  }
  var end = new Date();
  console.log(res);
  console.log(res.data.length);
  console.log("time : " + (end -start) + "/ " + TEST_NUMBER);
  
  console.log("\ninnerJoin test 2k X 2k...");
  var res;
  var start = new  Date();
  for(var i=0; i<TEST_NUMBER; i++){
    if(i % 2000 == 0) console.log(i);
    res = innerJoin(ta, tb, on);
  }
  var end = new Date();
  console.log(res);
  console.log(res.data.length);
  console.log("time : " + (end -start) + "/ " + TEST_NUMBER);
}

test();
