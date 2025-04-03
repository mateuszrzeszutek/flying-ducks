plugins {
  kotlin("jvm") version "2.1.20"
  application

  id("com.google.protobuf") version "0.9.5"
}

repositories {
  mavenCentral()
}

dependencies {
  implementation(libs.duckdb)
  implementation(libs.flight.sql)
  implementation(libs.guava)
  implementation(libs.protobuf)
  implementation(libs.slf4j.api)
  runtimeOnly(libs.slf4j.simple)

  testImplementation(libs.junit.jupiter)
  testRuntimeOnly("org.junit.platform:junit-platform-launcher")
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
}

tasks.named<Test>("test") {
  useJUnitPlatform()
}
