/**
 * @author : windyrobin@Gmail.com(Edward Zhang) 
 * @date:  12/31/2011 
 * 
 */

var on = {
  left : ["id", "pid"],
  right: ["id", "tid"]
  //left : ["id"],
  //right: ["id"]
};

var ta = [];
var tb = [];

function dataInit(){
  for(var i=0; i< 2000; i++){
    var a = {};
    var b = {};

    //var rid = parseInt(Math.random() * 5000);
    a["id"] = i;
    rid = parseInt(Math.random() * 10);
    a["pid"] = rid;
    a["first"] = "hello";

    b["id"] = i;
    rid = parseInt(Math.random() * 10);
    b["tid"] = rid;
    b["last"] = "world";

    ta.push(a);
    tb.push(b);
  }
}

function leftJoin(ta ,tb, on){
  var ka = on.left;
  var kb = on.right;

  var klen = kb.length;
  var i, j;

  //hash b like that:
  // kb1 : [ele1, ele2]
  // kb2 : [el3, ele5];
  var hb = {};
  var tblen = tb.length;
  var ele;
  var bid;
  for(i=0; i<tblen; i++){
    ele = tb[i];
    bid = "";
    for(j=0; j<klen;j++){
      bid += ele[kb[j]];
      bid += ",";
    }
    if(hb[bid] == null) hb[bid] = [];
    hb[bid].push(ele);
    //console.log("bhash index :" + bid);
  }

  var dummyhb = [];
  var emptyb = {};
  //ele is now one b element
  for(key in ele){
    emptyb[key] = "";
  }
  dummyhb.push(emptyb);

  var res = [];
  var talen = ta.length;
  var aele;
  var bele;
  var key;
  var hba;
  var bingo = 0;
  for(i=0; i<talen; i++){
    aele = ta[i];

    //get the related b elements array
    key = "";
    for(j=0; j<klen; j++){
      key += aele[ka[j]]; 
      key += ","
    }
    //console.log("ta.index : " + key);
    hba = hb[key];

    if(hba == null) hba = dummyhb;
    else bingo++;

    for(j=0; j<hba.length; j++){
      ele = {};    
      bele = hba[j];

      for(key in bele){
        ele[key] = bele[key];
      }
      for(key in aele){
        ele[key] = aele[key];
      } 
      res.push(ele);
    }
  }

  //console.log("bingo : " + bingo);
  return res;
}

var TEST_NUMBER = 10000;
function test(){
  dataInit();
  var start = new  Date();
  var res;
  for(var i=0; i<TEST_NUMBER; i++){
    if(i % 2000 == 0) console.log(i);
    res = leftJoin(ta, tb, on);
  }
  var end = new Date();
  console.log(res);
  console.log(res.length);
  console.log("time : " + (end -start));
}

test();
