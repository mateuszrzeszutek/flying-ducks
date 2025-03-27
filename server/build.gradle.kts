plugins {
  kotlin("jvm") version "2.1.20"
  application
}

repositories {
  mavenCentral()
}

dependencies {
  implementation(libs.guava)
  implementation(libs.flight.sql)

  testImplementation(libs.junit.jupiter)
  testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

kotlin {
  jvmToolchain(21)
}

application {
  mainClass = "io.rzeszut.flyingducks.Main"
}

tasks.named<Test>("test") {
  useJUnitPlatform()
}
