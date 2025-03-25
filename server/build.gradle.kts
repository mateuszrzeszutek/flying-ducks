plugins {
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

java {
  toolchain {
    languageVersion = JavaLanguageVersion.of(21)
  }
}

application {
  mainClass = "org.example.App"
}

tasks.named<Test>("test") {
  useJUnitPlatform()
}
