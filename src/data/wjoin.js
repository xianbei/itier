// (C) 2011-2012 Alibaba Group Holding Limited.
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU General Public License 
// version 2 as published by the Free Software Foundation. 

// Authors :fengyin <fengyin.zym@taobao.com>

exports.leftJoin  = leftJoin;
exports.innerJoin = innerJoin;

function leftJoin(ta ,tb, on){
  var ka = on.left;
  var kb = on.right;
  
  var klen = kb.length;
  var i, j, k;

  //hash b like that:
  // kb1 : [ele1, ele2]
  // kb2 : [el3, ele5];
  var adata = ta.data;
  var bdata = tb.data;
  var tblen = bdata.length;
  var talen = adata.length;
  
  var ele;
  var aele;
  var bele;
  var key;
  var hba;

  var apos = [];
  var acols = ta.columns;
  for(i=0; i<klen; i++){
    apos.push(acols.indexOf(ka[i]));
  }


  var bpos = [];
  var bcols = tb.columns;
  for(i=0; i<klen; i++){
    bpos.push(bcols.indexOf(kb[i]));
  }

  //console.log("bcols :" + bcols);
  //console.log("bpos :" + bpos);

  var rcols = [].concat(acols);
  var diffpos = [];
  for(i=0; i<bcols.length; i++){
    if((kb.indexOf(bcols[i]) < 0) &&(acols.indexOf(bcols[i]) < 0)){
      rcols.push(bcols[i]);
      diffpos.push(i);
    }
  }
  var difflen = diffpos.length;

  //console.log("rcols :" + rcols);
  //console.log("diffpos :" + diffpos);


  var dummyhb = [];
  for(i=0; i<bcols.length; i++){
    dummyhb.push("");
  }
  dummyhb = [dummyhb];

  ka = apos;
  kb = bpos;
  
  var hb = {};
  for(i=0; i<tblen; i++){
    bele = bdata[i];
    key = "";
    for(j=0; j<klen; j++){
      key += bele[kb[j]];
      key += ",";
    }
    if(hb[key] == null) hb[key] = [];
    hb[key].push(bele);
    //console.log("bhash index :" + bid);
  }

  //result data
  var rdata = [];

  for(i=0; i<talen; i++){
    aele = adata[i];

    //get the related b elements array
    key = "";
    for(j=0; j<klen; j++){
      key += aele[ka[j]]; 
      key += ","
    }
    //console.log("ta.index : " + key);
    hba = hb[key];

    if(hba == null) hba = dummyhb;
    //else bingo++;

    for(j=0; j<hba.length; j++){
      ele = [].concat(aele);    
      //ele = (aele);    
      bele = hba[j];

      for(k=0; k<difflen; k++){
        ele.push(bele[diffpos[k]]);
      }
      rdata.push(ele);
    }
  }

  //console.log("bingo : " + bingo);
  return {
    columns : rcols,
    data    : rdata
  };
}

function innerJoin(ta ,tb, on){
  var ka = on.left;
  var kb = on.right;
  
  var klen = kb.length;
  var i, j, k;

  //hash b like that:
  // kb1 : [ele1, ele2]
  // kb2 : [el3, ele5];
  var adata = ta.data;
  var bdata = tb.data;
  var tblen = bdata.length;
  var talen = adata.length;
  
  var ele;
  var aele;
  var bele;
  var key;
  var hba;

  var apos = [];
  var acols = ta.columns;
  for(i=0; i<klen; i++){
    apos.push(acols.indexOf(ka[i]));
  }


  var bpos = [];
  var bcols = tb.columns;
  for(i=0; i<klen; i++){
    bpos.push(bcols.indexOf(kb[i]));
  }

  //console.log("bcols :" + bcols);
  //console.log("bpos :" + bpos);

  var rcols = [].concat(acols);
  var diffpos = [];
  for(i=0; i<bcols.length; i++){
    if((kb.indexOf(bcols[i]) < 0) &&(acols.indexOf(bcols[i]) < 0)){
      rcols.push(bcols[i]);
      diffpos.push(i);
    }
  }
  var difflen = diffpos.length;

  //console.log("rcols :" + rcols);
  //console.log("diffpos :" + diffpos);


  ka = apos;
  kb = bpos;
  
  var hb = {};
  for(i=0; i<tblen; i++){
    bele = bdata[i];
    key = "";
    for(j=0; j<klen; j++){
      key += bele[kb[j]];
      key += ",";
    }
    hb[key] = bele;
    //console.log("bhash index :" + bid);
  }

  //result data
  var rdata = [];

  for(i=0; i<talen; i++){
    aele = adata[i];

    //get the related b elements array
    key = "";
    for(j=0; j<klen; j++){
      key += aele[ka[j]]; 
      key += ","
    }
    //console.log("ta.index : " + key);
    bele = hb[key];

    if(bele){
      ele = [].concat(aele);    
      //ele = (aele);    
      for(k=0; k<difflen; k++){
        ele.push(bele[diffpos[k]]);
      }
      rdata.push(ele);
    }
  }

  //console.log("bingo : " + bingo);
  return {
    columns : rcols,
    data    : rdata
  };
}

