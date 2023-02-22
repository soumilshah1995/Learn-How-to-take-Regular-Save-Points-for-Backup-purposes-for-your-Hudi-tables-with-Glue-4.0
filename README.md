# Learn How to take Regular Save Points for Backup purposes for your Hudi tables  with Glue 4.0

![image](https://user-images.githubusercontent.com/39345855/220790899-d2076743-9390-4602-9e5b-319a66591927.png)

## Glue job Settings 
![image](https://user-images.githubusercontent.com/39345855/220790961-fa486aab-9c43-4cce-8f17-301f27737f2c.png)

![image](https://user-images.githubusercontent.com/39345855/220791042-a1c18393-776f-42bd-ab1d-859f3cdb5520.png)

```
--additional-python-modules  faker==11.3.0

--conf --conf spark.serializer=org.apache.spark.serializer.KryoSerializer --conf spark.sql.hive.convertMetastoreParquet=false --conf spark.sql.hive.convertMetastoreParquet=false --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog --conf spark.sql.legacy.pathOptionBehavior.enabled=true
--datalake-formats hudi

```
