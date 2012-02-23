var http = require('http');
var url  = require('url');

//convert data to format like below
//{
//  columns : ['id', 'name' ,'type'],
//  data : [
//    [1, 'n1', 't1'],
//    [2, 'n2', 't2'],
//    [3, 'n3', 't3'],
//    ...
//  ]
//}
function dataFormat(rows){
  var res = {
    columns : [],
    data : []
  };

  //rows meust be an array
  var fnode = rows[0];
   
  if(fnode){
    var key;
    for(key in fnode){
      res.columns.push(key);
    }
    var ele;
    var data = res.data;
    var len = rows.length,i=0;
    while( i < len){
      fnode = rows[i];
      ele = [];
      for(key in fnode){
        ele.push(fnode[key]);
      }
      data.push(ele);
	  i++;
    }
  }
  return res;
}

/*
 **create a service for query all 'male' or 'female'
 **@input  : http://host:port/?fm=[m|f]
 **@output : formated JSON data
 */
http.createServer(function(req, res){
  var data = [
    {'id' : 1,  'sex' : 'm'},
    {'id' : 2,  'sex' : 'm'},
    {'id' : 3,  'sex' : 'f'},
    {'id' : 4,  'sex' : 'f'},
    {'id' : 5,  'sex' : 'm'},
    {'id' : 6,  'sex' : 'f'},
    {'id' : 7,  'sex' : 'm'},
    {'id' : 8,  'sex' : 'f'},
    {'id' : 9,  'sex' : 'm'},
    {'id' : 10, 'sex' : 'f'}
  ];
  var params = url.parse(req.url, true).query;

  var fm = params['fm'];
  var arr = [];
  for(var i = 0; i < data.length; i++){
    if(data[i].sex == fm)
    arr.push(data[i]);
  }

  var str = JSON.stringify(dataFormat(arr));
  res.writeHead(200, {
    'Content-type' : 'application/json',
    'Content-length' : str.length
  });

  res.end(str);

}).listen(3561)

/*
 **create a service for query items greater than some 'id'
 **@input  : http://host:port/?minId=[id]
 **@output : formated JSON data
 */
http.createServer(function(req, res){
  var data = [
    {'id' : 1,  'name' : 'n1',  'type' : 't1'},
    {'id' : 2,  'name' : 'n2',  'type' : 't2'},
    {'id' : 3,  'name' : 'n3',  'type' : 't3'},
    {'id' : 4,  'name' : 'n4',  'type' : 't4'},
    {'id' : 5,  'name' : 'n5',  'type' : 't5'},
    {'id' : 6,  'name' : 'n6',  'type' : 't6'},
    {'id' : 7,  'name' : 'n7',  'type' : 't7'},
    {'id' : 8,  'name' : 'n8',  'type' : 't8'},
    {'id' : 9,  'name' : 'n9',  'type' : 't9'},
    {'id' : 10, 'name' : 'n10', 'type' : 't10'}
  ];
  var params = url.parse(req.url, true).query;

  var minId = parseInt(params['minId']);
  var arr = [];
  for(var i = minId; i < data.length; i++){
    arr.push(data[i]);
  }

  var str = JSON.stringify(dataFormat(arr));
  res.writeHead(200, {
    'Content-type' : 'application/json',
    'Content-length' : str.length
  });

  res.end(str);

}).listen(3562)
