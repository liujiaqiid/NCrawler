/**
 *
 *  NCrawler
 */
var
  HDF = require("../configs/hdf"),
  request = require('request'),
  Q = require("q"),
  util = require("util"),
  _ = require('underscore'),
  Department = require('../models/Department');
/**
 * 查询某个地区下面的所有医院列表
 *
 */
exports.getDepartmentListByHospitalId = function (hos) {

  var deferred = Q.defer();
  if (hos == undefined){
    return;
  }

  var path = HDF.getHospitalFacultyListByHospitalId;
  var queryString =
    _.reduce(
      _.map(_.extend(HDF.query, {hospitalId: hos.id}),
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
        console.log("ReqError:"+error);
        deferred.resolve("");
      }
      //console.log("id:" + hospitalId)
      body = JSON.parse(body);
     // body.id = hospitalId;
      deferred.resolve({body:body,hospital: hos});
    });

  return deferred.promise;
};

exports.getDepartmentId = function (index) {
  return Department.find({}, "_id id name provinceId provinceName hospitalId hospitalName").exec();
};

/**
 * 查询某个地区下面的所有医院列表
 *
 */
exports.getDepartmentListByHospitalIdAndUpdate = function (hospitalId, updates) {
  return Department.update({hospitalId: hospitalId}, updates, {multi: true}).exec();
};

/**
 * 解析并存储JSON数据
 * @param json
 * @returns {*}
 */
exports.parseAndStore = function (json){
  var id = json.hospital._id;
  console.log("Begin data parse and store function. " + id);
  var deferred = Q.defer();


  var content = json.body.content;
  if (id == undefined){
    console.log("id is undefined");
    return;
  }
  console.log("content:" + content);
  if (content.length > 0){
    content = _.forEach(content, function(d){
      return _.extend(d, {hospitalId: id,
        hospitalName: json.hospital.name,
        provinceName: json.hospital.provinceName,
        provinceId: json.hospital.provinceId});
    });

    return Department.create(content)
      .then(function (result) {
        //console.log("Create success: " + result);
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
  return Department.find(con, fields).exec();
};

exports.create = function(spl){
  return Department.create(spl);
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


exports.updateIds = function(conds, updates){
 //console.log("Conds:"+ JSON.stringify(conds) +";Updates:"+JSON.stringify(updates));
return Department.update(conds,{'$set':updates},{multi:true}).exec();
};
