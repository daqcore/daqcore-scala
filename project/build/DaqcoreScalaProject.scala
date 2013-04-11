import sbt._
import Process._

class DaqcoreScalaProject(info: ProjectInfo) extends DefaultProject(info) with AutoCompilerPlugins {
  override def parallelExecution = true

  val publishTo = Resolver.file("maven-local", Path.userHome / ".m2" / "repository" asFile)
  val mavenLocal = "Local Maven Repository" at "file://"+Path.userHome+"/.m2/repository"

  val daqcoreReleases = "DAQCorE Releases" at "http://daqcore.github.io/repo/releases/"

  val typesafeReso = "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"
  val jBossRepo = "jBoss" at "http://repository.jboss.org/nexus/content/groups/public/"

  val scala_continuations = compilerPlugin("org.scala-lang.plugins" % "continuations" % "2.9.0-1")

  val scala_swing = "org.scala-lang" % "scala-swing" % "2.9.0-1"

  val scalatest = "org.scalatest" % "scalatest_2.9.0" % "1.4.1" % "test"

  val akka_actor = "se.scalablesolutions.akka" % "akka-actor" % "1.1.3"
  val akka_remote = "se.scalablesolutions.akka" % "akka-remote" % "1.1.3"
  val akka_stm = "se.scalablesolutions.akka" % "akka-stm" % "1.1.3"
  val akka_slf4j = "se.scalablesolutions.akka" % "akka-slf4j" % "1.1.3"

  val dispatch_http = "net.databinder" % "dispatch-http_2.9.0" % "0.8.3" % "compile"
  val dispatch_http_json = "net.databinder" % "dispatch-http-json_2.9.0" % "0.8.3" % "compile"
  val dispatch_couch = "net.databinder" % "dispatch-couch_2.9.0" % "0.8.3" % "compile"
  
  val logback_core = "ch.qos.logback" % "logback-core" % "0.9.28"
  val logback_classic = "ch.qos.logback" % "logback-classic" % "0.9.28" % "runtime" intransitive()
  val log4j_over_slf4j = "org.slf4j" % "log4j-over-slf4j" % "1.6.0" % "runtime" intransitive()

  val netty = "org.jboss.netty" % "netty" % "3.2.1.Final" % "compile"

  val jfreechart = "jfree" % "jfreechart" % "1.0.13"

  val xstream = "com.thoughtworks.xstream" % "xstream" % "1.3.1"
  val jettison = "org.codehaus.jettison" % "jettison" % "1.0.1"

  val colt = "colt" % "colt" % "1.2.0"
  val remotetea_oncrpc = "org.acplt" % "remotetea-oncrpc" % "1.0.7"
  val snmp4j = "org.snmp4j" % "snmp4j" % "1.11.3"
  val mibble_parser = "net.percederberg.mibble" % "mibble-parser" % "2.9.2"
  val mibble_mibs = "net.percederberg.mibble" % "mibble-mibs" % "2.9.2" % "runtime"

  def javaSrcDir = "src" / "main" / "java"

  def jrpcgenTask(pkgName: String, client: String, server: String, xdrName: String) = {
    def xdrSrcDir = "src" / "main" / "xdr"
    def xdrFile = xdrSrcDir / xdrName
    val targetDir = pkgName.split('.').foldLeft (javaSrcDir) ((path, part) => path / part)

    val cleanSrc = cleanTask(targetDir) named("clean-oncrpc-" + pkgName)
    execTask {
      FileUtilities.createDirectory(targetDir, log)
      <x>jrpcgen -ser -nobackup -d {targetDir.absolutePath} -p {pkgName} -c {client} -s {server} {xdrFile}</x>
    } dependsOn(cleanSrc)
  }

  lazy val rpcgen_vxi11core = jrpcgenTask("daqcore.io.oncrpc.vxi11core", "Client", "Server", "vxi11core.x") describedAs("Generate vxi11core classes and stubs.")
  lazy val rpcgen = task {None} dependsOn(rpcgen_vxi11core)
  
  override def compileOptions = super.compileOptions ++
    Seq(Unchecked) ++ // Enable unchecked warnings
    compileOptions("-P:continuations:enable") // Enable continuations plugin
  
  // override def compileOrder = CompileOrder.JavaThenScala

  //// Scaladoc workaround for sbt + scala-2.8
  //private val prjDocTitle = projectName + " " + projectVersion + " API Docs"
  //override def documentOptions = Seq(CompoundDocOption("-doc-title", prjDocTitle))
}
