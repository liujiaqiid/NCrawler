/**
 *
 *  NCrawler
 */
var
  util = require("util"),
  HDF = require("../configs/hdf"),
  request = require('request'),
  Q = require("q"),
  _ = require('underscore'),
  Hospital = require('../models/Hospital');
/**
 * 查询某个地区下面的所有医院列表
 *
 * @param province  省、市地址
 *
 */
exports.getHospitalListByProvince = function (province) {

  console.log("Begin getHospitalListByProvince");
  var deferred = Q.defer();
  alias = province.alias;
  var path = HDF.getHospitalListByProvince;
  var queryString =
    _.reduce(
      _.map(_.extend(HDF.query, {province: alias}),
        function (value, key) {
          return key + "=" + value;
        }),
      function (memo, value) {
        return memo + "&" + value;
      });
  var url = HDF.host + path + queryString;

  console.log("QueryString: " + url);

  request(
    { method: 'GET'
      , uri: url
      , gzip: true
    },
    function (error, response, body) {
      console.log("statusCode" + response.statusCode);
      if (error){
        console.log(error);
        deferred.resolve('');
      }
      deferred.resolve({body:body,province: province});
    })
    .on('data', function(data) {
      // decompressed data as it is received
      console.log('decoded chunk: ' + data.length);
    });
  return deferred.promise;
};

exports.getHospitalId = function () {
  return Hospital.find({}, "_id id name provinceId province provinceName").exec();
};

/**
 * 解析并存储JSON数据
 * @param json
 * @returns {*}
 */
exports.parseAndStore = function (json){

  console.log("Begin data parse and store function. " + json.length);
  var deferred = Q.defer();
  var raw = JSON.parse(json.body);
  var province = json.province;
  var rawContent = raw.content;

  console.log(util.inspect(province));
  rawContent = _.each(rawContent, function(d){
    return _.extend(d,{provinceName:province.name, provinceId: province.id});
  });

  console.log("content:" + rawContent.length);
  if (rawContent.length > 0){
    return Hospital.create(rawContent)
      .then(function (result) {
        console.log("Create success: " + result);
        deferred.resolve(result);
        return deferred.promise;
      }, function (err) {
        console.log("Create error: "+err);
        deferred.reject(err);
        return deferred.promise;
      });
  }else{
    console.log("Create is null!!");
    deferred.resolve();
    return deferred.promise;
  }
};

exports.find = function (con, fields){
  return Hospital.find(con, fields).exec();
};


exports.create = function(spl){
  return Hospital.create(spl);
};

//{
//  signature: ""
//  errorCode: "0"
//  msg: "正常"
//  content: [
//    {
//    id: "335"
//    name: "301医院"
//    district: "海淀"
//    doctorCount: "1379"
//    grade: "6"
//    featuredFaculties: "综合"
//    province: "北京"
//    caseDoctorCount: "536"
//    bookingDoctorCount: "117"
//    _s: "D31bdQJpA2FQb11rAj0MJVciAGACMgAjBmxScQQyVT8MM1x-VmMDNgRmBGdRNAxnBSdWflIlUz4AMQVwCGYHKA9zWzMCYAMsUDtdLgJnDGhXeQA9AmYAMAZhUmEEMFUsDClcclYjA2sEJgQlUWgMPwV2VjdSU1M7AD0FYQgtB2YPJFtmAjEDPFAwXTwCZwxjV2QANgJmACMGKw"
//    }
//  ]
//}
