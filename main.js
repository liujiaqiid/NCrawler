/**
 *  数据抓取demo
 */
var
  Hospital = require('./app/controllers/HospitalController'),
  _ = require('underscore'),
  util = require('util'),
  DoctorList = require('./app/controllers/DoctorListController'),
  Supplier = require('./app/controllers/SupplierController'),
  Doctor = require('./app/controllers/DoctorController'),
  Index = require('./app/models/Index.js'),
  Department = require('./app/controllers/DepartmentController'),
  Faculty = require('./app/controllers/FacultyController'),
  SubFaculty = require('./app/models/SubFaculty'),
  HDF = require("./app/configs/hdf"),
  DiseaseController = require('./app/controllers/DiseaseController.js'),
  ProfileController = require('./app/controllers/ProfileController.js');
  PROVINCELIST = require('./app/configs/province.js').PROVINCEES,
  RegionController = require('./app/controllers/RegionController.js'),
  DISTINCTDOCIDS = require('./app/configs/distinctDocId.js').docIds,
  HOSIDS = require('./app/configs/distinctDocId.js').HosIDS,
  Region = require('./app/models/Region.js');

var privinceId = "5509080d8faee0fbe0c4a6e0";
var province = {
    "id" : "5509080d8faee0fbe0c4a6e0",
    "createdAt" : 1426655247511,
    "updatedAt" : 1426655247511,
    "isDeleted" : false,
    "type" : 1.0000000000000000,
    "areaId" : "520000",
    "name" : "贵州省",
    "alias" : "贵州"
};

console.log("Crawler Begin Working....");

////////////////////////////////////////////////////////////////////////
//////////////////////// 基础数据抓取 /////////////////////////////////////
////////////////////////////////////////////////////////////////////////

/**
 * 1. 查询并存储全国所有的医院  Hospital
 * Tip:  将PROVINCELIST[i]修改为对应省得信息

Hospital.getHospitalListByProvince(province)    //? 需要在此步骤中关省市信息？？？？？？？
  .then(function(data){
    console.log("Finish get data.");
    return Hospital.parseAndStore(data);
  })
  .then(function(){
    console.log("Finish parse and store data.");
  },function(err){
    console.log("oooo:" + err);
  });

*/


/**
 * 2. 获取所有医院科室并关联该科室对应的医院信息  Department
 *
 Hospital.getHospitalId()
  .then(function (ids) {
    var hosList = JSON.parse(JSON.stringify(ids));
    var idsArr = _.pluck(ids, 'id');

    var i = 0 ;
    var timer = setInterval(function(){

      (function(i){

        if (i >= hosList.length){
          console.log('Game over');
          clearInterval(timer);
          return;
        }

        var hos = hosList[i];
        //通过医院的id来查询医院科室信息
        Department.getDepartmentListByHospitalId(hos)
        .then(function (data) {

          console.log("Finish get data.") + id;
          return Department.parseAndStore(data);
        })
        .then(function () {

          console.log("Finish parse and store data."+id);
        }, function (err) {
          console.log("oooo:" + err);
        });

      })(i++);

    }, 200);

  });
*/

/**
 * 3. 查询所有科室医生列表--医生基本信息  DoctorList
 *   每四秒 call 一次hdf api，防治ip被封锁
 *

Department.getDepartmentId()
  .then(function (depts) {
    //var idsArr = _.pluck(ids, 'id');
    depts = JSON.parse(JSON.stringify(depts));
    console.log("##### " + depts.length);

    var i = 0 ;
    var timer = setInterval(function(){

      (function(i){

        if (i >= depts.length){
          console.log('Game over');
          clearInterval(timer);
          return;
        }

        var dept = depts[i];
        DoctorList.getDoctorListByDepartmentId(dept)
          .then(function (data) {
            console.log("Finish get data.");
            return DoctorList.parseAndStore(data, data.departmentId);
          })
          .then(function () {
            console.log("Finish parse and store data.");
          }, function (err) {
            console.log("oooo:" + err);
          });

      })(i++);

    }, 2000);

  });

*/

/**
 * 4. 查询所有医生详情   Doctor
 *    每1s爬取数据一次防止被hdf封锁
 *

DoctorList.getId()
 .then(function (ids) {

   var i = 0;
   var idsArr = _.pluck(ids, 'id');

   console.log("##### " + idsArr.length);

   var timer = setInterval(function(){

     (function(i){

       if (i >= idsArr.length){
         console.log('Game over');
         clearInterval(timer);
         return;
       }

       var id = idsArr[i];

       Doctor.getDoctorInfoByDoctorId(id)
        .then(function(data){
          console.log("Finish get data.");
          return Doctor.parseAndStore(data);
        })
        .then(function(){
          console.log("Finish parse and store data.");
        },function(err){
          console.log("!!!!!! Error:oooo:" + err);
        });

     })(i++);

   }, 100);

 });

*/

////////////////////////////////////////////////////////////////////////
//////////////////////// 关系数据抓取、整理 ///////////////////////////////
////////////////////////////////////////////////////////////////////////


/**
 * 10. 关联疾病一级科室、二级科室、疾病 与 医生 更新医生列表 (关系)
 *
 * Tip:修改Doctor.getDoctorListByDiseaseKey 方法querySring的privince为相应地省份或者直辖市
 * 直辖市，例如：北京市，去掉-市
 * 省份，例如：山东省，去掉-省
 *
//Step1: 获取所有的疾病列表
DiseaseController.getDiseaseList()
  .then(function(list){

    console.log("length: " + list.length);
    var i = 0 ;

    var timer = setInterval(function(){

    (function(i){

      if( i >= list.length){
       console.log("Game over...");
       clearInterval(timer);
       return;
      }

      var key = list[i].key;

      var relation = {
        func: 1,
        facultyId: list[i].facultyId,
        facultyName: list[i].facultyName,
        facultyKey: list[i].facultyKey,
        subFacultyId: list[i].subFacultyId,
        subFacultyName: list[i].subFacultyName,
        diseaseId: list[i]._id,
        diseaseKey: key,
        diseaseName: list[i].name
      };

      Doctor.getDoctorListByDiseaseKey(key, relation, "贵州")
        .then(function (result){
          var doctorList = (JSON.parse(result.data)).content;
          var relation = result.relation;
          var relationList = [];
          var hdfID;
          for (var index in doctorList){
            hdfID = doctorList[index].id;
            relationList.push(
              _.extend(
                _.clone(relation), {doctorId: hdfID}));
          }
          return Doctor.create(relationList);
        })
        .then(function(){
          console.log("Create Success");
        }, function(err){
          console.log("!!!!!!Err: " + err);
        });

    })(i++)


  }, 100);
  });
*/

/**
 * 11. 新增北京索引
 *     在region表找到对应的地区信息
 */
/*
Index.create([{
"_id" : "5509080d8faee0fbe0c4a6df",
"name" : "四川省",
"isDeleted" : false,
"updatedAt" : 1426655247511,
"createdAt" : 1426655247511,
"source" : "zly",
"type" : 1
}]);

{
    "_id" : ObjectId("5509080d8faee0fbe0c4a6e0"),
    "createdAt" : 1426655247511.0000000000000000,
    "updatedAt" : 1426655247511.0000000000000000,
    "isDeleted" : false,
    "type" : 1.0000000000000000,
    "areaId" : "520000",
    "name" : "贵州省"
}
*/


/*
15. 更新医生关系信息  根据省、医院、科室拉出来的数据都是默认func=0, 所以这里面需要批量更新func= 2
    func=2的医生数据是func=1的医生数据的超集
    Tip:在数据库里面执行脚本
        修改provinceName为对应的省份  Region表查询 provinceId修改为对应的ID
        将provinceId 和 provinceName修改为对应的省*/
/*
db.doctors.update(
{func:0},
{$set: {func:2, provinceId: "5509080d8faee0fbe0c4a6e0", provinceName:"贵州"}},
{multi:true});
*/



/**
 * 16. 通过hdf的id关联地点科室与DoctorRelation
 *     将 对应的医院、科室信息关联到按照地点索引的医生Document里面
 *

Department.getDepartmentId()
  .then(function(data){

    var list = JSON.parse(JSON.stringify(data));
    console.log("#####" + list.length);

    for (var i = 0; i < list.length; i++) {
      var hs = list[i];
      var updates = {
        //provinceId: hs.provinceId,
        //provinceName: hs.province,
        hospitalId: hs.hospitalId,
        hospitalName: hs.hospitalName,
        departmentId: hs._id,
        departmentName: hs.name
      };
      console.log("Update doctor: " + util.inspect(updates));
      Doctor.updateDoctor({func: 2, hospitalFacultyId: hs.id}, updates);

    }

  });

*/

/**
 * 21.2 Index合并表操作 Hospital
 *

var fields = "_id id name district gps doctorCount " +
  "grade featuredFaculties provinceId provinceName " +
  "caseDoctorCount bookingDoctorCount "

Hospital.find({}, fields)
  .then(function(data){

    var list = JSON.parse(JSON.stringify(data));
    console.log("#####" + list.length);
    var newList = [];

    for (var i = 0; i < list.length; i++) {

      var hos = list[i];

      newList.push(
        _.extend(
          _.clone(hos),{hdfId: hos.id, type:2}));
    }

    return Index.create(newList);
  })
  .then(function(){
    console.log("Success")
  },function(err){
    console.log("!!!!!!!Err:"+err);
  });

*/

/**
* 21.3 Index合并表操作 Department
*

var fields = "_id id provinceId provinceName hospitalId " +
  "hospitalName name doctorCount category order " +
  "caseDoctorCount bookingDoctorCount ";

Department.find({}, fields)
  .then(function(data){

    var list = JSON.parse(JSON.stringify(data));
    console.log("#####" + list.length);
    var newList = [];
    for (var i = 0; i < list.length; i++) {
      var dep = list[i];

      newList.push(
        _.extend(
          _.clone(dep),{hdfId: dep.id, type:3}));
    }
    //console.log("List: " + util.inspect(newList));
    Index.create(newList);
  });

*/

/**
 * 从doctor表里面dictinct func=2[fun=2的医生包含func=1的医生]医生数据，批量创建profile表
 * 数据库查找对应的dosIds
 *
 * 1. 在doctor集合中查询 func=2(地点索引的医生)的 hdf医生id
 * 2. 遍历所有医生的hdf ID, 查询并转存到profile表
 *
 *

Doctor.getDisDoctorIds('doctorId',{func:2})
  .then(function(dosIds){

    //_.values(dosIds);//DISTINCTDOCIDS);
    dosIds = dosIds || [];
    console.log("Begin: " + dosIds);
    dosIds.forEach(function(d){

      Doctor.findOne({doctorId: d, func:2})
        .then(function(doc){
          if (!doc) return;
          var doctor = JSON.parse(JSON.stringify(doc));
          var profile = {};

          profile.type = doctor.type || 0;
          profile.source = doctor.sourceType || "hdf";
          profile.createdAt = doctor.createdAt || Date.now();
          profile.updatedAt = doctor.updatedAt || Date.now();
          profile.isDeleted = doctor.isDeleted || false;
          profile.name = doctor.name || "";
          profile.alias = doctor.alias || "";
          profile.pinyin = doctor.pinyin || [];
          profile.avatar =doctor.avatar || "";
          profile.sex = doctor.sex || "";
          profile.doctorIntro = doctor.doctorIntro || "";
          profile.contact = doctor.contact || "";
          profile.address = doctor.address || "";
          profile.position = doctor.position || "";
          profile.fullGrade = doctor.fullGrade || "";
          profile.specialize = doctor.specialize || "";
          profile.practicingNumber =doctor.practicingNumber || "";
          profile.descriptionImages = doctor.descriptionImages || [];
          profile.descriptionTags = doctor.descriptionTags || [];
          profile.searchTags = doctor.searchTags || [];
          profile.eduBackground = doctor.eduBackground || [];
          profile.brokerActiveNum = doctor.brokerActiveNum || 0;
          profile.serviceActiveNum = doctor.serviceActiveNum || 0;
          profile.brokerId = doctor.brokerId || "";
          profile.checkStatus = doctor.checkStatus || 10;
          profile.failReason = doctor.failReason || "";
          profile.hdfId = doctor.hdfId || "";
          profile.doctorNo = doctor.doctorNo || "";
          profile.hospitalFacultyId = doctor.hospitalFacultyId || "";
          profile.doctorId = doctor.doctorId || "";

         return Doctor.CreateProfile(profile);

       })
       .then(function(d){
         console.log("Create Success!")
       }, function (err){
         console.log("!!!Err: " + err)
       });

     });
 });

*/


/**
 * 基于医院id查找对应的区域信息，然后查找医院对应的科室列表，
 * 基于每一个科室下面的医生列表更新profile表relatedHospital字段
 *
 * 当拉去省数据时候：
 * RegionController 里面需拼接：济南市
 * 当拉取直辖市数据的时候：
 *  RegionController 里面需拼接：朝阳区
 *
 *  1. 查询所有医院,遍历医院,
 *  2. 查询医院对应的 区域信息
 *  3. 查询医院对应的 科室信息
 *  4. 更新科室对应的医生信息
 *
 *

Hospital.find({},'_id id district')
  .then(function(hospitals){
    console.log(hospitals.length);
    hospitals.forEach(function(hospital){

      //console.log(hospital);
      RegionController.find(hospital)
        .then(function(region){
          var body = region.body;
          var districId = body._id;
          var districtName = body.name;
          var hospitalId = region.hos._id;

          Department.find({hospitalId:hospitalId})
            .then(function(deps){

              console.log("#####"+deps.length);
              deps.forEach(function(dep){
                var data = {
                 provinceId: dep.provinceId,
                 provinceName: dep.provinceName,
                 districtId: districId,
                 districtName: districtName,
                 hospitalId: dep.hospitalId,
                 hospitalName: dep.hospitalName,
                 departmentId: dep._id,
                 departmentName: dep.name
               };

               return Doctor.updateDoctorProfileRelatedHospital({hospitalFacultyId:dep.id},data)
               .then(function(d){
                 console.log("Add relatedHospital Success!");
               }, function (err){
                 console.log("!!!Err1 : " + err);
               });
             });
           }, function (err){
             console.log("!!!Err2 : " + err);
           })
         }, function (err){
           console.log("!!!Err3 : " + err);
         });
       });
});

*/

/**
 * 查找func = 1[疾病索引出来]的医生信息，然后更新对应profile表relatedDisease字段
 *
 * 1. 查询有主治疾病的医生, 遍历医生
 * 2. 查询医生的主治疾病
 * 3. 更新医生的主治字段
 *
 *

var i = 0;
Doctor.getDisDoctorIds('doctorId',{func:1})
  .then(function(docs){

    console.log(" *******Distinct Doc length:"+ docs.length);
    _.values(docs).forEach(function(id){
      console.log(id);
      return Doctor.find({doctorId: id, func:1})
        .then(function(doctors){

         console.log("Doctor length########"+ doctors.length);
         doctors.forEach(function(doc){

          var data = {
           facultyId: doc.facultyId,
           facultyName: doc.facultyName,
           subFacultyId: doc.subFacultyId,
           subFacultyName: doc.subFacultyName,
           diseaseId: doc.diseaseId,
           diseaseName: doc.diseaseName
          };

          return Doctor.updateDoctorProfileRelatedDisease({doctorId:doc.doctorId},data)
           .then(function(d){
             ++i;
             console.log("Add RelatedDisease Success!"+ i);
          }, function(err){
            console.log("!!!!Err1 : " + err);
          });
         });
      }, function (err) {
        console.log("!!!!Err2 : " + err);
      });
    });
}, function (err) {
  console.log("!!!!Err3 : " + err);
});
*/

/**
 *
 * 2. 将医院关联到相应地地区下面
 * tip:  Region表查出对应省的privinceId
 *
 *
 *  !!!!需要手动更新!!!!!!
 *
 *
Region.find({type:2,"provinceId" : privinceId}).exec()
  .then(function(rs){
    rs.forEach(function(r){
      var disName = r.name.substring(0, r.name.length-1);
      var disId = r._id;
      console.log("DisName:"+disName+";disId:"+disId);
      var updates = {
        districtId:disId,
        districtName: r.name
      };

      var cons ={
       type:2,
       provinceId : privinceId,
       district : disName
      };

      Index.update(cons,{$set:updates},{multi:true}).exec();
    });
  });
*/


/***

//mongoexport --host 182.92.81.107 --port 27017 -d hdf -c indexes -q "{}" --csv --out ./indexes.csv --fields _id,source,type,provinceId,provinceName,districtId,districtName,name,gps,grade,isDeleted,updatedAt,createdAt,hdfId,district,doctorCount,featuredFaculties,caseDoctorCount,bookingDoctorCount
//mongoimport --host localhost --port 27017 --db zlyweb --collection indexes --type csv --headerline --file ./indexes.csv

//107
mongodump -d hdf -c indexes -o ./tmp/
mongodump -d hdf -c profiles -o ./tmp/

scp -r zlycare@182.92.81.107:~/tmp ./

//DB3
mongorestore -d zlyweb -c indexes ./tmp/hdf/indexes.bson
mongorestore -d zlyweb -c profiles ./tmp/hdf/profiles.bson

//
db.regions.find({_id:ObjectId("5509080d8faee0fbe0c4a6e0")})
db.regions.update({provinceId:"5509080d8faee0fbe0c4a6e0"},{$set: {hospitalNum:1}},{multi:true})
*/
