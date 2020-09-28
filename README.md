# elasticsearch_client (elasticsearch orm frame)
---------------------------------------------------------

 /**
  * 单线程数据插入(插入对象为src/test/com.elasticsearch.vo.Person)
  * 参考示例代码：src/test/com.elasticsearch.insert.InsertTest.test1()
  */

 /**
  * 多线程数据插入(插入对象为src/test/com.elasticsearch.vo.Person)
  * 参考示例代码：src/test/com.elasticsearch.insert.InsertTest.test2()
  */

 /**
  * 父子类数据插入
  *  (插入父对象为com.elasticsearch.po.joinPo.MyJoinIndex
  *   插入子对象为com.elasticsearch.po.joinPo.SonObject)
  * 参考示例代码：src/test/com.elasticsearch.insert.InsertTest.test3()
  */
---------------------------------------------------------

 /**
  * 根据查询删除数据
  * 参考示例代码：src/test/com.elasticsearch.delete.DeleteTest.test1()
  */

 /**
  * 根据id删除数据
  * 参考示例代码：src/test/com.elasticsearch.delete.DeleteTest.test2()
  */
---------------------------------------------------------

 /**
  * 根据查询修改数据
  * 参考示例代码：src/test/com.elasticsearch.update.UpdateTest.test1()
  */
---------------------------------------------------------

 /**
  * 条件组合查询(支持and、or、isNull、isNotNull、in、like、no_like、大于、大于等于、小于、小于等于、不等于、范围)
  * 支持 a=1 and (b=2 or b=3) 查询
  * 支持 ( a=1 and (b=2 or b=3)) or (a=2 and (b=1 or b =2)) 查询
  * 请自行进行查询组合,组合完成的json串请参看日志信息。
  * 参考示例代码：src/test/com.elasticsearch.select.SelectTest.test1()
  */

 /**
  * 分组查询(按字段分组)
  * 参考示例代码：src/test/com.elasticsearch.select.SelectTest.test2()
  */

 /**
  * 分组查询(按时间分组)
  * 参考示例代码：src/test/com.elasticsearch.select.SelectTest.test3()
  */
 
 /**
  * scroll遍历(支持条件过滤后获取全部数据 || 遍历索引全部数据)
  * 参考示例代码：src/test/com.elasticsearch.select.SelectTest.test4()
  */

 /**
  * 嵌套分组查询(先根据时间分组,然后很据字段分组)
  * 参考示例代码：src/test/com.elasticsearch.select.SelectTest.test5()
  */

 /**
  * 嵌套分组查询(先根据字段分组,然后很据时间分组)
  * 参考示例代码：src/test/com.elasticsearch.select.SelectTest.test6()
  */

 /**
  * 自定义查询DSL语句查询
  * 参考示例代码：src/test/com.elasticsearch.select.SelectTest.test7()
  */
---------------------------------------------------------

  

  
  
     


