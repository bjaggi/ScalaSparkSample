#this is a boilerplate spark/scala code , with sbt and scalatests
# i intend to write a sentimental analysis program using this approach
# To run this on windown instal hadoop-comon (winutins.exe)
# to run on cluster do spark-submit
#step 1 to setup code on you machine (install git)
git clone https://github.com/bjaggi/SparkScalaDemo.git

#step 2 (install sbt)
sbt package or sbt run

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
