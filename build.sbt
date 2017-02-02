// factor out common settings into a sequence
lazy val commonSettings = Seq(
  organization := "com.eva",
  version := "0.1.0",
  // set the Scala version used for the project
  scalaVersion := "2.11.8",
  javacOptions in (Compile, compile) ++= Seq("-source", "1.7", "-target", "1.7")
  
)

// define ModuleID for library dependencies
lazy val scalacheck = "org.scalacheck" %% "scalacheck" % "1.12.0"


lazy val root = (project in file(".")).
  settings(
    name := "SparkScalaDemo",
    version := "1.0",
    scalaVersion := "2.11.8",
    
    scalaSource in Compile := baseDirectory.value / "src/main",

    // set the Scala test source directory to be <base>/test
    scalaSource in Test := baseDirectory.value / "src/test",

    // add a test dependency on ScalaCheck
    libraryDependencies += scalacheck % Test,

    // add compile dependency on osmlib
    //libraryDependencies += osmlib,
     mainClass in (Compile, packageBin) := Some("com.eva.app.WordCount"),

    // set the main class for the main 'run' task
    // change Compile to Test to set it for 'test:run'
    mainClass in (Compile, run) := Some("com.eva.app.WordCount"),
    
libraryDependencies ++= Seq(
      "log4j" % "log4j" % "1.2.15" excludeAll(
        ExclusionRule(organization = "com.sun.jdmk"),
        ExclusionRule(organization = "com.sun.jmx"),
        ExclusionRule(organization = "javax.jms")
        ),
        
        
      "org.apache.hadoop" % "hadoop-common" % "2.7.1" % "provided" excludeAll ExclusionRule(organization = "javax.servlet"),

		"org.apache.spark" % "spark-core_2.11" % "1.4.1" % "provided",
		//#"org.apache.spark" % "spark-streaming" % sparkVer % "provided",
		//#"org.apache.spark" % "spark-streaming-kafka" % sparkVer exclude("org.apache.spark", "spark-streaming_2.11")
		"org.apache.spark" %% "spark-hive" % "1.4.1" % "provided",
		"org.apache.spark" %% "spark-hive" % "1.4.1" % "provided",

        //"com.databricks" %% "spark-csv" % "1.4.0",
        //"com.databricks" %% "spark-avro" % "2.0.1",
		//"com.julianpeeters" %% "case-class-generator" % "0.7.1",

		"junit" % "junit" % "4.12" ,
		//"com.novocode" % "junit-interface" % "0.11" % "test",
		"org.scalatest" %% "scalatest" % "3.0.1",
		"org.scalactic" %% "scalactic" % "3.0.1",
		"org.scalatest" %% "scalatest" % "3.0.1" % "test"
		
		 
		
		
      
      )
	//unmanagedJars in Compile += file("lib/EUtils_1.0.jar"),

	
  )