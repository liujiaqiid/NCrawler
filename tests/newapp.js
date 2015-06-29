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

console.log("Crawler Begin Working....");

////////////////////////////////////////////////////////////////////////
//////////////////////// 基础数据抓取 /////////////////////////////////////
////////////////////////////////////////////////////////////////////////

/**
 * 1. 查询并存储全国所有的医院  Hospital
 * Tip:  将PROVINCELIST[i]修改为对应省得信息
 */
var i = 0;
var temp ;
temp = setInterval(function(){
if(i==1){
  clearInterval(temp);
  return;
};
Hospital.getHospitalListByProvince(PROVINCELIST[i])
  .then(function(data){
   console.log("Finish get data.");
   ++i;
   return Hospital.parseAndStore(data);
  }).then(function(){
   console.log("Finish parse and store data.");
  }, function(err){
   console.log("oooo:" + err);
  });
},100);


/**
 * 2. 获取所有医院科室并关联该科室对应的医院信息  Department
 */
/*
Hospital.getHospitalId()
.then(function(ids){
  var i = 0;
  //var idsArr = _.pluck(ids, 'id');
  //console.log("Hospital list ##### : " + ids);
  var temp;
  temp = setInterval(function(){
   if( i == ids.length){
    clearInterval(temp);
    return;
   };
   var id = ids[i];
   ++i;
   Department.getDepartmentListByHospitalId(id)
    .then(function(data){
     console.log("Finish get data.");

     return Department.parseAndStore(data);
    }, function(err){
     console.log("oooo:" + err);
    });

  },1);
});
*/

/**
 * 3. 查询所有科室医生列表--医生基本信息  DoctorList
 *   每四秒 call 一次hdf api，防治ip被封锁
*/
/*
Department.getDepartmentId()
  .then(function (ids) {
   // var idsArr = _.pluck(ids, 'id');
  //  console.log("##### " + idsArr);
    var i = 0 ;
    var temp = setInterval(function(){
      if(i == ids.length){
       clearInterval(temp);
       return;
      }
     var data = ids[i];
     ++i;
      DoctorList.getDoctorListByDepartmentId(data)
        .then(function (data) {
          console.log("Finish get data.");
          return DoctorList.parseAndStore(data, data.departmentId);
        })
        .then(function () {
          console.log("Finish parse and store data.");
        }, function (err) {
          console.log("oooo:" + err);
        });
    },10);
  });
*/

/**
 * 4. 查询所有医生详情   Doctor
 *    每1s爬取数据一次防止被hdf封锁
 */
/*
DoctorList.getId()
  .then(function (ids) {
//var ids2 = JSON.parse(JSON.stringify(ids));
// console.log(ids);
    var idsArr = _.pluck(ids, 'id');
   console.log("##### " + idsArr.length);
   var i = 0;
   var temp ;

    temp   = setInterval(function(){
    if(i==idsArr.length){
      clearInterval(temp);
      return;
     };
     var id = idsArr[i];
     ++i;
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
    },1);
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
 */
/*
DiseaseController.getDiseaseList()
  .then(function(list){
    console.log("length: " + list.length);
    var data = 0 ;
    var temp = setInterval(function(){
     if( data == list.length){
      clearInterval(temp);
      return;
     };
      var key = list[data].key;
      var relation = {
        func: 1,
        facultyId: list[data].facultyId,
        facultyName: list[data].facultyName,
        facultyKey: list[data].facultyKey,
        subFacultyId: list[data].subFacultyId,
        subFacultyName: list[data].subFacultyName,
        diseaseId: list[data]._id,
        diseaseKey: key,
        diseaseName: list[data].name
      };
      ++data;
      Doctor.getDoctorListByDiseaseKey(key, relation)
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
    },1);
  });*/

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
*/


/*
15. 更新医生关系信息  根据省、医院、科室拉出来的数据都是默认func=0, 所以这里面需要批量更新func= 2
    func=2的医生数据是func=1的医生数据的超集
    Tip:在数据库里面执行脚本
        修改provinceName为对应的省份  Region表查询 provinceId修改为对应的ID
        将provinceId 和 provinceName修改为对应的省*/
/*db.doctors.update(
{func:0},
{$set: {func:2, provinceId: "5509080d8faee0fbe0c4a6cb", provinceName:"河北"}},
{multi:true});*/



/**
 * 16. 通过hdf的id关联地点科室与DoctorRelation
 * 将 对应的医院、科室信息关联到按照地点索引的医生Document里面
 */
/*
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
 */
/*
Hospital.find({},
  "_id id name district gps doctorCount grade featuredFaculties provinceId provinceName " +
    " caseDoctorCount bookingDoctorCount ")
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
    //console.log("List: " + util.inspect(newList));
    return Index.create(newList);
  })
  .then(function(){
    console.log("Success")
  },function(err){
    console.log("!!!!!!!Err:"+err);
  });*/


///**
// * 21.3 Index合并表操作 Department
// */
/*
Department.find({},
  "_id id provinceId provinceName hospitalId hospitalName name doctorCount " +
    " category order caseDoctorCount bookingDoctorCount ")
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
  });*/


/**
 * 从doctor表里面dictinct func=2[fun=2的医生包含func=1的医生]医生数据，批量创建profile表
 * 数据库查找对应的dosIds
 */
/*
Doctor.getDisDoctorIds('doctorId',{func:2}).then(function(dosIds){
 var dosIds = _.values(DISTINCTDOCIDS);
 dosIds.forEach(function(d){
   Doctor.find({doctorId: d, func:2}).then(function(doc){
    var doctor = JSON.parse(JSON.stringify(doc[0]));
    console.log("Doctors :"+ doctor);
    var profile = {};
    if(doctor.type != undefined){
     profile.type = doctor.type;
     // console.log("###########"+profile.type);
     //console.log("#####"+ doctor.type);
    }
    if(doctor.sourceType)
     profile.source = doctor.sourceType;
    if(doctor.createdAt)
     profile.createdAt = doctor.createdAt;
    if(doctor.updatedAt)
     profile.updatedAt = doctor.updatedAt;
    if(doctor.isDeleted != undefined)
     profile.isDeleted = doctor.isDeleted;
    if(doctor.name)
     profile.name = doctor.name;
    if(doctor.alias)
     profile.alias = doctor.alias;
    if(doctor.pinyin)
     profile.pinyin = doctor.pinyin;
    if(doctor.avatar)
     profile.avatar =doctor.avatar;
    if(doctor.sex)
     profile.sex = doctor.sex;
    if(doctor.doctorIntro)
     profile.doctorIntro = doctor.doctorIntro;
    if(doctor.contact)
     profile.contact = doctor.contact;
    if(doctor.address)
     profile.address = doctor.address;
    if(doctor.position)
     profile.position = doctor.position;
    if(doctor.fullGrade)
     profile.fullGrade = doctor.fullGrade;
    if(doctor.specialize)
     profile.specialize = doctor.specialize;
    if(doctor.practicingNumber)
     profile.practicingNumber =doctor.practicingNumber;
    if(doctor.descriptionImages)
     profile.descriptionImages = doctor.descriptionImages;
    if(doctor.descriptionTags)
     profile.descriptionTags = doctor.descriptionTags;
    if(doctor.searchTags)
     profile.searchTags = doctor.searchTags;
    if(doctor.eduBackground)
     profile.eduBackground = doctor.eduBackground;
    if(doctor.brokerActiveNum)
     profile.brokerActiveNum = doctor.brokerActiveNum;
    if(doctor.serviceActiveNum)
     profile.serviceActiveNum = doctor.serviceActiveNum;
    if(doctor.brokerId)
     profile.brokerId = doctor.brokerId;
    if(doctor.checkStatus != undefined)
     profile.checkStatus = doctor.checkStatus;
    if(doctor.failReason)
     profile.failReason = doctor.failReason;
    if(doctor.hdfId)
     profile.hdfId = doctor.hdfId;
    if(doctor.doctorNo)
     profile.doctorNo = doctor.doctorNo;
    if(doctor.hospitalFacultyId != undefined)
     profile.hospitalFacultyId = doctor.hospitalFacultyId;
    if(doctor.doctorId != undefined)
     profile.doctorId = doctor.doctorId;

    return Doctor.CreateProfile(profile).then(function(d){
     console.log("Create Success!")
    });
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
 */
/*
Hospital.find({},'_id id district').then(function(hospitals){
hospitals.forEach(function(hospital){
  console.log(hospital);
RegionController.find(hospital).then(function(region){
  var body = region.body;
  var districId = body._id;
  var districtName = body.name;
  var hospitalId = region.hos._id;

  Department.find({hospitalId:hospitalId}).then(function(deps){

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

   return Doctor.updateDoctorProfileRelatedHospital({hospitalFacultyId:dep.id},data).then(function(d){
   console.log("Add relatedHospital Success!");
   });
   });
})
});
});
});
*/

/**
 * 查找func = 1[疾病索引出来]的医生信息，然后更新对应profile表relatedDisease字段
 */
/*
var i = 0;
Doctor.getDisDoctorIds('doctorId',{func:1}).then(function(docs){
console.log(" *******Distinct Doc length:"+ docs.length);
_.values(docs).forEach(function(id){
  return Doctor.find({doctorId: id, func:1}).then(function(doctors){
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
    });
   });
  });
});
});
*/

/**
 *
 * 2. 将医院关联到相应地地区下面
 * tip:  Region表查出对应省的privinceId
 */
/*var privinceId = "5509080d8faee0fbe0c4a6d2";

Region.find({type:2,"provinceId" : privinceId}).exec().then(function(rs){
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
   "provinceId" : privinceId,
   "district" : disName
  };

  Index.update(cons,{$set:updates},{multi:true}).exec();
  ;
});

});*/
