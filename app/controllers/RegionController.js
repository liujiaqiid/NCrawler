/**
 * Created by cao on 15/4/8.
 */

var Region = require('../models/Region.js'),
 Q = require('q'),
 _ = require('underscore');

exports.find = function(hos){
 var name = hos.district+"市";
 var deferred = Q.defer();

 return Region.find({name:name}).exec()
 .then(function(r){
   if(!r){
    console.log("*******Err: No this region : ******" + name);
    deferred.resolve("");
   };

   deferred.resolve({body: r[0], hos:hos});
   return deferred.promise;
 }, function(err){
   console.log("!!!Err: " + err);
   deferred.reject(err);
   return deferred.promise;
 });
};
