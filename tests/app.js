/**
 *  数据抓取demo
 */
var
  _ = require('underscore'),
  util = require('util'),
  Hospital = require('./app/controllers/HospitalController'),
  Department = require('./app/controllers/DepartmentController'),
  DoctorList = require('./app/controllers/DoctorListController'),
  Supplier = require('./app/controllers/SupplierController'),
  Doctor = require('./app/controllers/DoctorController'),
  Index = require('./app/models/Index'),
  Faculty = require('./app/controllers/FacultyController'),
  SubFaculty = require('./app/models/SubFaculty'),
  HDF = require("./app/configs/hdf"),
  DiseaseController = require('./app/controllers/DiseaseController.js'),
  ProfileController = require('./app/controllers/ProfileController.js');

console.log("Crawler Begin Working....");

////////////////////////////////////////////////////////////////////////
//////////////////////// 基础数据抓取 /////////////////////////////////////
////////////////////////////////////////////////////////////////////////

/**
 * 1. 查询并存储北京所有的医院  Hospital

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
 * 2. 获取所有医院科室  Department

Hospital.getHospitalId()
 .then(function (ids) {
   var hosList = JSON.parse(JSON.stringify(ids));
   var idsArr = _.pluck(ids, 'id');

   //遍历医院
   hosList.forEach(function(hos){

     var id = hos.id;
     console.log("Hospital id ##### : " + id);

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
   });

 });

*/

/**
 * 3. 查询所有科室医生列表--医生基本信息  DoctorList
 *   每四秒 call 一次hdf api，防治ip被封锁

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
//////////////////////// 疾病索引 ///////////////////////////////
////////////////////////////////////////////////////////////////////////

/**
 * 5. 一级科室 - 初始化疾病一级科室

 Faculty.initFaculty(HDF.FACULTY_KEYS)
  .then(function(){
    console.log("Success init");
  }, function(err){
    console.log("!!!!!!Err init : " + err);
  });
 */

/**
 * 6. 二级科室 - 查询并存储疾病二级科室列表 SubFaculty

var keys = HDF.FACULTY_KEYS;//疾病以及科室名已经存为常量
 //console.log("keys: "+keys);
 for(var key in keys){//遍历所有key值
   console.log("key:"+ key);
   Faculty.getDiseaseFacultyListByFacultyKey(key)
   .then(function(data){
     console.log("Finish get data.");
     return Faculty.parseAndStore(data.data,data.key);
   })
   .then(function(){
     console.log("Finish parse and store data.");
   },function(err){
     console.log("oooo:" + err);
   });
 };
 */

/**
 * 7. 疾病 - 通过疾病二级科室编号获取疾病列表 + 科室ID  Disease

SubFaculty.find({},{id:1}).exec().then(function(ids){
  console.log("ids.length:"+ids.length);
  for(var id in ids){
    console.log(ids[id].id);
    DiseaseController.getDiseaseListByFacultyId(ids[id].id)
      .then(function(data){
        console.log("Finish get data.");
        return DiseaseController.parseAndStore(data.data,data.id);
      })
      .then(function(){
        console.log("Finish parse and store data.");
      },function(err){
        console.log("oooo:" + err);
      });
  };
});
*/


////////////////////////////////////////////////////////////////////////
//////////////////////// 关系数据抓取、整理 ///////////////////////////////
////////////////////////////////////////////////////////////////////////


/**
 * 8. 关联疾病一级科室 与 二级科室, 批量更新二级科室

Faculty.connectFacultyWithSub();  代码中key是否修改为facultyKey?
//Format batch
db.subfaculties.find({}).forEach(function(d){
 var key = d.key;
 d.facultyKey = key;
 db.subfaculties.save(d);
})

*/

/**
 * 9. 关联疾病一级科室、二级科室 与 疾病
Faculty.connectFacSubWithDis();
*/

/**
 * 10. 关联疾病一级科室、二级科室、疾病 与 医生 更新医生列表 (关系)

DiseaseController.getDiseaseList()
 .then(function(list){
   //console.log("Finish get disease list")
   console.log("length: " + list.length);
   //for (var data in list){
   var data = -1;
   setInterval(function(){
     data++;
     console.log("********"+data);
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
     Doctor.getDoctorListByDiseaseKey(key, relation)
       .then(function (result){
         //console.log("Result return! " + util.inspect(result) );
         var doctorList = (JSON.parse(result.data)).content;
         var relation = result.relation;
         var relationList = [];
         var hdfID;
         for (var index in doctorList){
           hdfID = doctorList[index].id;
           //console.log( index + " : id : " + hdfID);
           relationList.push(
             _.extend(
               _.clone(relation), {doctorId: hdfID}));
         }
         //console.log("==========================data: " + util.inspect(relationList));
         return Doctor.create(relationList);
       })
       .then(function(){
         console.log("Create Success");
       }, function(err){
         console.log("!!!!!!Err: " + err);
       });
   },3000);
   //}
 });
 
*/


/**
 * 11. 新增北京索引
 */
//Index.create([{
//  "_id" : ObjectId("54b8bbd551f77c2d2a029402"),
//  "name" : "北京",
//  "isDeleted" : false,
//  "updatedAt" : 1421391992497,
//  "createdAt" : 1421391992497,
//  "source" : "zly",
//  "type" : 1
//}]);

/**
 * 12. 关联北京-医院
 *  MongoDB语句
 */
//db.hospitals.update(
//  {province:"北京"},
//  {$set: {provinceId: "54b8bbd551f77c2d2a029402"}},
//  {multi:true});

/**
 * 13. 关联医院-科室
 *     更新所有科室数据,关联到对应的医院_id
 */
/*Hospital.getHospitalId()
  .then(function (data) {
    var list = JSON.parse(JSON.stringify(data));
    //var idsArr = _.pluck(ids, 'id');
    console.log("#####" + list.length);
    for (var i = 0; i < list.length; i++) {
      var hs = list[i];
      var updates = {
        provinceId: hs.provinceId,
        provinceName: hs.province,
        hospitalId: hs._id,
        hospitalName: hs.name
      };
      console.log(util.inspect(updates));
      Department.getDepartmentListByHospitalIdAndUpdate(hs.id, updates)
        .then(function (data) {
          console.log("Finish get data.");
        }, function (err) {
          console.log("oooo:" + err);
        });
    }
  });*/

//
// 14. 更新医院信息
//db.hospitals.update(
//  {province:"北京"},
//  {$set: {provinceId: "54b8bbd551f77c2d2a029402"}},
//  {multi:true});

//
// 15. 更新医生关系信息
//db.doctors.update(
//  {func:0},
//  {$set: {func:2, provinceId: "54b8bbd551f77c2d2a029402", provinceName:"北京"}},
//  {multi:true});
//


/**
 * 16. 通过hdf的id关联地点科室与DoctorRelation
 */
/*Department.getDepartmentId()
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
      Doctor.updateDoctor({func: 2, hospitalFacultyId: hs.id}, updates);   //这里是departmentID吧？
    }
  });*/

// 17. 将所有func=2的医生关系记录中的doctorId字段置为空
// db.doctors.update({func: 2}, {$set: {doctorId: ""}}, {multi:true})

/**
* 18. 科室关系  关联到医生profile表 Step1
* 从func=2的doctor信息中提取医生profile，并存储
* 提取单点执业的医生profile
*/
//Doctor.find({func: 2, name: {$nin: HDF.DUPLICATE_NAMES}})
//  .then(function (data) {
//    var list = JSON.parse(JSON.stringify(data));
//    console.log("#####" + list.length);
//    for (var i = 0; i < list.length; i++) {
//      var hs = list[i];
//      var id = hs._id;
//      delete hs._id;
//      delete hs.__v;
//
//      ProfileController.create(hs, id)
//        .then(function(data){
//          var newProfileId = data.profile._id;
//          var relationId = data.id;
//          var conds = {
//            func:2,
//            _id: relationId
//          };
//          var updates = {
//            doctorId: newProfileId
//          };
//          console.log("Cond: " + util.inspect(conds) + " Updates: " + util.inspect(updates));
//          Doctor.updateDoctor(conds, updates)
//            .then(function(){
//            }, function (err){
//              console.log("!!!!!!!UpdateErr:"+err);
//            });
//        }, function(err){
//          console.log("!!!!!!!CreateErr:"+err);
//        });
//
//    }
//  });

/**
 * 19. 科室关系  关联到医生profile表 Step2
 * 从func=2的doctor信息中提取医生profile，并存储
 * 提取非单点执业的医生profile
 */
//Doctor.find({func: 2, name: {$in: HDF.DUPLICATE_NAMES}})
//  .then(function (data) {
//    var list = JSON.parse(JSON.stringify(data));
//    console.log("#####" + list.length);
//    //for (var i = 0; i < list.length; i++) {
//    var num = -1;
//    setInterval(function(){
//      num++;
//      if (num >= list.length) return;
//      var hs = list[num];
//      //var id = hs._id;
//      //判断医生唯一的标准: 姓名相同 + 个人简介相同 + 个人专长投票信息相同
//      var name  = hs.name;
//      var intro = hs.doctorIntro;//个人简介信息
//      var vote  = hs.diseaseVotes;//个人专长投票信息
//      delete hs.__v;
//      var con = {
//        name: name,
//        doctorIntro: intro,
//        diseaseVotes: vote
//      };
//      console.log("Profile conditions: " + util.inspect(con));
//      ProfileController.find(con, hs)
//        .then(function(data){
//          if (data.profile){
//            console.log("Exists!!");
//            var conds = {
//              func:2,
//              _id: data.param._id
//            };
//            var updates = {
//              doctorId: data.profile._id
//            };
//            Doctor.updateDoctor(conds,updates)
//              .then(function(){
//              }, function (err){
//                console.log("!!!!!!!UpdateErr:"+err);
//              });
//          }else{
//            var pro = data.param;
//            var id  = pro._id;
//            delete pro._id;
//            ProfileController.create(pro, id)
//              .then(function(data){
//                var newProfileId = data.profile._id;
//                var relationId = data.id;
//                var conds = {
//                  func:2,
//                  _id: relationId
//                };
//                var updates = {
//                  doctorId: newProfileId
//                };
//                console.log("Cond: " + util.inspect(conds) + " Updates: " + util.inspect(updates));
//                Doctor.updateDoctor(conds, updates)
//                  .then(function(){
//                  }, function (err){
//                    console.log("!!!!!!!UpdateErr:"+err);
//                  });
//              }, function(err){
//                console.log("!!!!!!!CreateErr:"+err);
//              });
//          }
//        }, function(err){
//          console.log("!!!!!!!FindErr:"+err);
//        });
//
//    }, 100);
//  });

/**
 * 20. 疾病关系 通过科室关系 关联到profile表
 */
//Doctor.find({func: 2, id:{$in: HDF.TMP_LIST}},
//  {_id: 0, id: 1, doctorId: 1, name: 1, logoUrl: 1})
//  .then(function (data){
//    var list = JSON.parse(JSON.stringify(data));
//    console.log("#####" + list.length);
//    for (var i = 0; i < list.length; i++) {
//      var d = list[i];
//      var con = {
//        func: 1,
//        doctorId: d.id//,
//        //doctorName: {$exists: false}
//      };
//      var updates = {
//        doctorId: d.doctorId,
//        doctorName: d.name//,
//        //doctorIntro: d.doctorIntro,
//        //logoUrl: d.logoUrl
//      };
//      //console.log(i + "update " + util.inspect(updates));
//      Doctor.updateDoctor(con, updates)
//        .then(function(){
//          console.log("update success!");
//        },function(err){
//          console.log("!!!!!!!UpdateErr:"+err);
//        });
//    }
//  }, function(err){
//    console.log("!!!!!!!FindErr:"+err);
//  });
//
//
//  有部分为关联上的关系,标识删除  MongoDB脚本
//db.doctors.find(  {func:1, doctorName:{$exists: false}});
//db.doctors.update(
//  {func:1, doctorName:{$exists: false}},
//  {$set: {isDeleted: true}},
//  {multi: true});

/**
 * 21.1 Index合并表操作 Province
 */
//Index.create([{
//  "_id" : ObjectId("54b8bbd551f77c2d2a029402"),
//  "name" : "北京",
//  "isDeleted" : false,
//  "updatedAt" : 1421391992497,
//  "createdAt" : 1421391992497,
//  "source" : "zly",
//  "type" : 1
//}]);
/**
 * 21.2 Index合并表操作 Hospital
 */
//Hospital.find({},
//  "_id id name district gps doctorCount grade featuredFaculties provinceId provinceName " +
//    " caseDoctorCount bookingDoctorCount ")
//  .then(function(data){
//    var list = JSON.parse(JSON.stringify(data));
//    console.log("#####" + list.length);
//    var newList = [];
//    for (var i = 0; i < list.length; i++) {
//      var hos = list[i];
//
//      newList.push(
//        _.extend(
//          _.clone(hos),{hdfId: hos.id, type:2}));
//    }
//    //console.log("List: " + util.inspect(newList));
//    return Index.create(newList);
//  })
//  .then(function(){
//    console.log("Success")
//  },function(err){
//    console.log("!!!!!!!Err:"+err);
//  });

///**
// * 21.3 Index合并表操作 Department
// */
//Department.find({},
//  "_id id provinceId provinceName hospitalId hospitalName name doctorCount " +
//    " category order caseDoctorCount bookingDoctorCount ")
//  .then(function(data){
//    var list = JSON.parse(JSON.stringify(data));
//    console.log("#####" + list.length);
//    var newList = [];
//    for (var i = 0; i < list.length; i++) {
//      var dep = list[i];
//
//      newList.push(
//        _.extend(
//          _.clone(dep),{hdfId: dep.id, type:3}));
//    }
//    //console.log("List: " + util.inspect(newList));
//    Index.create(newList);
//  });

///**
// * 21.4 Index合并表操作 Faculty
// */
//Faculty.find({},
//    "_id key name ")
//  .then(function(data){
//    var list = JSON.parse(JSON.stringify(data));
//    console.log("#####" + list.length);
//    var newList = [];
//    for (var i = 0; i < list.length; i++) {
//      var fac = list[i];
//
//      newList.push(
//        _.extend(
//          _.clone(fac),{type:4}));
//    }
//    console.log("List: " + util.inspect(newList));
//    Index.create(newList);
//  });

/**
* 21.5 Index合并表操作 SubFaculty
*/
//SubFaculty.find({},
//  "_id id name facultyId facultyKey facultyName").exec()
//  .then(function(data){
//    var list = JSON.parse(JSON.stringify(data));
//    console.log("#####" + list.length);
//    var newList = [];
//    for (var i = 0; i < list.length; i++) {
//      var sub = list[i];
//
//      newList.push(
//        _.extend(
//          _.clone(sub),{hdfId: sub.id, type:5}));
//    }
//    //console.log("List: " + util.inspect(newList));
//    Index.create(newList);
//  });

/**
 * 21.6 Index合并表操作 Disease
 */
/*DiseaseController.find({},
  "_id id key name brief  diseaseDoctorCount spaceDoctorCount " +
    "facultyId facultyKey facultyName subFacultyId subFacultyName")
  .then(function(data){
    var list = JSON.parse(JSON.stringify(data));
    console.log("#####" + list.length);
    var newList = [];
    for (var i = 0; i < list.length; i++) {
      var dis = list[i];

      newList.push(
        _.extend(
          _.clone(dis),{hdfId: dis.id, type:6}));
    }
//    console.log("List: " + util.inspect(newList));
    Index.create(newList);
  });*/



/**
 * 提取 地点索引 与 医生的关系， 并在doctor中单独存储
 */
//var conds   = {func : 0};
//var fields  = "id name hospitalFacultyId hospitalFacultyName hospitalFacultyFullName " +
//  " hospitalId hospitalName";
//Doctor.getDoctorInfo(conds, fields)
//  .then(function(data){
//    var list = JSON.parse(JSON.stringify(data));
//    var relations = [];
//    var relation;
//    console.log("Length : " + list.length);
//    for (var i = 0; i < 10 ; i++){
//      relation = list[i];
//      delete relation._id;
//      relations.push(
//        _.extend(
//          _.clone(relation),
//          {func: 2, provinceId: "54b8bbd551f77c2d2a029402", provinceName:"北京"}));
//    }
//    //console.log("Data : " + util.inspect(relations));
//    Doctor.create(relations);
//  });
//Doctor.changeHdfId2DocMongoId();
//遍历疾病名 获取医生列表 更新现有医生关联的key


//function createSupplier(){
//  Doctor.find({})
//    .then(function(ds){
//      var arr = [];
//
//      ds.forEach(function(d){
//        var ss = d.fullGrade.split(" ");
//        var pos = ss[0]||ss[1];
//
//        var s = {
//          _id: d._id,
//          name: d.name,
//          avatar: d.logoUrl,
//
//          doctorIntro: d.doctorIntro,
//          hospitalId: d.hospitalId,
//          hospital: d.hospitalName,
//          province: d.province,
//          departmentId: d.hospitalFacultyId,
//          department: d.hospitalFacultyName,
//          fullGrade: d.fullGrade,
//          position: pos,
//          specialize: d.specialize,
//          goodVoteCount: d.goodVoteCount
//        };
//
//        arr.push(s);
//      });
//
//      Supplier.create(arr);
//    });
//};
//
//function createDpmIndex(){
//  Department.find({})
//    .then(function(ds){
//      var arr = [];
//
//      ds.forEach(function(d){
//        var s = {
//          _id: d._id,
//          name: d.name,
//          type: 3,
//
//          hospitalId: d.hospitalId,
//          category: d.category,
//          order: d.order,
//
//          doctorCount: d.doctorCount,
//          caseDoctorCount: d.caseDoctorCount,
//          bookingDoctorCount: d.bookingDoctorCount
//        };
//
//        arr.push(s);
//      });
//
//      Index.create(arr);
//    });
//};
//
//function createHptIndex(){
//  Hospital.find({})
//    .then(function(ds){
//      var arr = [];
//
//      ds.forEach(function(d){
//        var s = {
//          _id: d._id,
//          name: d.name,
//          type: 2,
//
//          district: d.district,
//          grade: d.grade,
//          featuredFaculties: d.featuredFaculties,
//          province: d.province,
//
//          doctorCount: d.doctorCount,
//          caseDoctorCount: d.caseDoctorCount,
//          bookingDoctorCount: d.bookingDoctorCount
//        };
//
//        arr.push(s);
//      });
//
//      Index.create(arr);
//    });
//};
//
//function sleep(sleepTime) {
//  for(var start = Date.now(); Date.now() - start <= sleepTime; ) { }
//}

//createSupplier();
//createHptIndex();
//createDpmIndex();
