# CCIndex V1.1.2 User Manual

##File Packing List
The CCIndex V1.1.2 package consists of:
- ict.ocrabase.main.java.client
- ict.ocrabase.main.java.regionserver
- ict.ocrabase.main.java.test
- org.apache.hadoop.hbase.filter
- org.apache.hadoop.hbase.filter
- org.apache.hbase.coprocessor

The total overview like this:

 ![packing-list](https://cl.ly/1l0v0p0H2Q3W/packing_list.png)
 
##Installation
Prerequisites: the Hadoop and the HBase environment have been right configured 
- Copy the file CCIndex-1.1.2.jar to path : *$HADOOP_ENV$/share/hadoop/yarn* . like this:

![yarn-jar](https://cl.ly/2j3A1R3F262A/jar_yarn.png)

- Add the src file to your project if you want to debug your code.(if not, you should add the CCIndex-1.1.2.jar to your path ) 
- Add the flowing configuration to *$HBASE-ENV$/conf/hbase-site.xml*
```xml
    <property>
      <name>hbase.coprocessor.region.classes</name>             
      <value>org.apache.hbase.coprocessor.PutObserver,org.apache.hbase.coprocessor.DeleteObserver</value>
    </property>
```
Up to now, the environment have been successfully configured

##How to use 
There are two ways to use CCIndex. One is no bulkload, the other is bulkload.
###No bulkload
No bulkload means you have to put data to you table, when there are a large amount of data, it may consume huge time.
There have some example in the ict.ocrabase.main.java.test package.

Example:

*ict.ocrabase.main.java.test.CreateTableWithIndexTest.java*  create hbase table and the ccindex table.
*ict.ocrabase.main.java.test.PutTableWithIndexTest.java*  put data to the hbase table and the ccindex table
###Bulkload
Bulkload use map reduce to put data to the base table and the ccindex table

Example:

- First upload the TPC-H test data to the hdfs.

*bin/hadoop fs -put ../test-data  /index-data*
- *ict.ocrabase.main.java.test.CreateTableTestBulkload.java* create hbase table and the ccindex table
- import data
*ict.ocrabase.main.java.client.cli.Import.java* import the hdfs data to hbase, the configuration parameter like this:
```javascript
-s /index-data -ts real_table_with_index,SEMICOLON,f:c1:STRING:CCINDEX:real_table_with_index-f_c1,f:c2:STRING,f:c3:STRING,f:c4:STRING:CCINDEX
:real_table_with_index-f_c4,f:c5:STRING,f:c6:STRING,f:c7:STRING,f:c8:STRING -l 32
```
PS: there the index columns are c1 and c4
##Query
- Scan without index
Example:

*ict.ocrabase.main.java.test.QueryByCondition.java*
- Scan with index
Example:

*ict.ocrabase.main.java.test.QueryMultiColumnUseCCIndex.java*

##Simple query test 
- Data source: TPC-H ORDERS table
- Total 20 million line data
- Index column c1(CUSTKEY) and c4(ORDERDATE)

tbale1

|               | CCIndex       | HBase  |
| ------------- |:-------------:| ------:|
| time(ms)      | 19962         |101593  |

tbale2

|               | CCIndex       | HBase  |
| ------------- |:-------------:| ------:|
| time(ms)      | 13841         |91988   |


Table1 only use c4 as query condition, the result count are 33514; 
Table2 use both c1 and c4 as query condition the result count are 13649;
