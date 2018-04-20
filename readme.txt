#this is a boilerplate spark/scala code , with sbt and scalatests
# To run this on windown instal hadoop-comon (winutins.exe)
# to run on cluster do spark-submit
#step 1 to setup code on you machine (install git)
git clone https://github.com/bjaggi/SparkScalaDemo.git

#step 2 (install sbt)
sbt package or sbt run

#step 2.1 to run this in eclipse do 
sbt eclipse

#step 3
change code (todo add unit tests)

# step 4 (review all code changed)
git status

# step 5 ( commit code with comments)
git commit -am "comments for code changed"

#step 6 ( run on spark)
spark-submit --class com.eva.app.HiveJoin --master local \sparkdemo.jar

#step 7 to build a package, go to root and do : sbt package
sbt package

#step 8 to test sbt
sbt test

#step9 to run sbt (not completely configured yet, probably not required as well)
sbt run

Ball Park Config
1> 4-6 cores per executor
1.a> if 90 cores on the cluster & we apply 5 cores per executor...we end up getting 90/5 = 18 executors
2> Leave one EXECUTOR (5 cores) for the YARN- Application manager
3> Partition size is usually 128MB
4> Each Node has 3 EXECUTORS (15 core Node)
5> 63GB/3 = 21GB (19GB memory each executor)...counting off head overhead.



Good Link : 
https://stackoverflow.com/questions/47445328/adding-a-new-column-to-a-dataframe-by-using-the-values-of-multiple-other-columns?rq=1&utm_medium=organic&utm_source=google_rich_qa&utm_campaign=google_rich_qa
