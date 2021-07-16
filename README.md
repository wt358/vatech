# Vatech songhyun

## 1. 전제
Apache Spark가 설치되어 있어야 하며, 해당 코드를 spark-submit <code.py>를 이용해 실행한다.

## 2. ETL
etl 폴더 안에 Faker 모듈을 이용한 dummy data 생성 코드와 해당 데이터를 가지고 ETL 과정을 거쳐 저장하는 코드 두개가 있다.

## 3. Thriftserver
SparkSQL을 다른 DB와 연결하기 위해서는 JDBC가 있어야 한다. Spark에서는 Thriftserver를 jdbc 서버로써 제공하는데, 이는 hiveserver2 기반(?)이다.

먼저 
```
SPARK_HOME/sbin/start-thriftserver.sh
```
를 통해 서버를 실행하고, 
```
SPARK_HOME/bin/beeline
``` 
을 통해 해당 서버에 접속한다. (혹은 spark-beeline)

기본 포트는 10000이므로, 
```
!connect jdbc:hive2://localhost:10000
```
 을 입력하면 Spark SQL에 접속가능하다.

## 4. Metabase
Metabase는 데이터베이스를 바탕으로 시각화해주는 오픈소스 BI 툴이다. Metabase 공식 사이트에서 jar 파일을 받아 실행한다. (<https://www.metabase.com/start/oss/>)
추후에 Spark 이미지를 만들어서 함께 띄울 예정
## 5. mySQL
Thriftserver를 통해 업로드 하는 것이 오류때문에 진행이 안되고 있어 mysql로 먼저 진행해보았다. mysql.py 파일을 submit 하는 것으로 해결되지만, spark 설치 경로 안의 jars 폴더에 jdbc connector 파일(jar)을 넣어줘야 정상적으로 실행된다. config로써 경로를 지정해주긴 했으나, 폴더에 넣어주거나, 인자를 주는 방법으로만 인식을 제대로 하는 듯 하다.
```
spark-submit --driver-class-path=path/to/mysql-connector-java-8.0.25.jar mysql.py
```
## 6. mongoDB
mongoDB는 JDBC를 사용하지 않더라도 스파크에서 기본으로 제공하기 때문에 spark-submit할 때 인자로 package를 다운받도록 하면 된다. 다만 
```
spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.1 mongo.py
```