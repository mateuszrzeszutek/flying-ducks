plugins {
  kotlin("jvm") version "2.1.20"
  application

  id("com.google.protobuf") version "0.9.5"
  id("me.champeau.jmh") version "0.7.2"
}

repositories {
  mavenCentral()
}

dependencies {
  implementation(libs.arrow.c.data)
  implementation(libs.flight.sql)
  implementation(libs.duckdb)
  implementation(libs.protobuf)
  implementation(libs.slf4j.api)
  runtimeOnly(libs.slf4j.simple)

  testImplementation(libs.flight.sql.adbc)
  testImplementation(libs.flight.sql.jdbc)
  testImplementation(libs.junit.jupiter)
  testImplementation(kotlin("test"))
  testRuntimeOnly("org.junit.platform:junit-platform-launcher")

  jmh(libs.flight.sql.adbc)
  jmh(libs.flight.sql.jdbc)
}

kotlin {
  jvmToolchain(21)
}

protobuf {
  protoc {
    artifact = "com.google.protobuf:protoc:${libs.versions.protobuf.get()}"
  }
}

application {
  mainClass = "io.rzeszut.flyingducks.Main"

  applicationDefaultJvmArgs += listOf(
    "--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED"
  )
}

tasks.named<Test>("test") {
  useJUnitPlatform()
  jvmArgs("--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED")
}

jmh {
  val databaseFile = project.rootDir.resolve("database.duckdb")

  jvmArgs.add("-Djmh.databaseFile=${databaseFile.absolutePath}")
  jvmArgs.add("--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED")
  jvmArgs.add("-Xmx2g")

  profilers.add("gc")
}
