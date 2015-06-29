/**
 *
 *  NCrawler
 */
var
  HDF = require("../configs/hdf"),
  request = require('request'),
  Q = require("q"),
  _ = require('underscore'),
  DoctorList = require('../models/DoctorList');

var
  pageSize = 1000, //每一页获取的数据
  pageId = 1;

/**
 * 查询某个医院科室医生列表
 *
 * @param departmentId  科室id
 *
 */
exports.getDoctorListByDepartmentId = function (data) {

  console.log("Begin getDoctorListByDepartmentId");

  var departmentId = data.id;

  if (departmentId == undefined) {
    //TODO 测试下，是否可以当id为undefined时返回所有科室医生信息？
    return;
  }

  var deferred = Q.defer();
  var path = HDF.getDoctorListByDepartmentId;
  var queryString =
  _.reduce(
    _.map(
      _.extend(HDF.query, {
        pageSize: pageSize,
        hospitalFacultyId: departmentId,
        pageId: pageId}),
      function (value, key) {
        return key + "=" + value;
      }),
      function (memo, value) {
        return memo + "&" + value;
      });

  var url = HDF.host + path + queryString;

  console.log("QueryString: " + url);

  request(
    {
      method: 'GET',
      uri: url,
      gzip: true
    },function (error, response, body) {
      //console.log("statusCode" + response.statusCode);
      if (error) {
      console.log("!!!!!ReqError: "+error);
      deferred.resolve('');
      };

      console.log("departmentId:" + departmentId);
      body = JSON.parse(body);
      body.departmentId = data._id;
      deferred.resolve(body);
    })
    .on('data', function (data) {
      // decompressed data as it is received
      console.log('decoded chunk: ' + data.length);
    });

  return deferred.promise;

};

exports.getId = function () {
  return DoctorList.find({}, "-_id id departmentId").exec();
};

/**
 * 解析并存储JSON数据
 * @param json
 * @returns {*}
 */
exports.parseAndStore = function (json, departmentId) {

  console.log("Begin data parse and store function. " + json.length);

  var deferred = Q.defer();
  var content = json.content;
  console.log("content:" + content.length);

  if (content.length > 0) {

    for (var i = 0; i < content.length; i++) {
      _.extend(content[i], {departmentId: departmentId});
    }

    console.log("---" + content[0]);

    return DoctorList.create(content)
      .then(function (result) {
        //console.log("Create success: " + result);
        deferred.resolve(result);
        return deferred.promise;
      }, function (err) {
        console.log("!!!!!Create error: " + err);
        deferred.reject(err);
        return deferred.promise;
      });
  } else {
    console.log("Create is null!!");
    deferred.resolve();
    return deferred.promise;
  }
};
