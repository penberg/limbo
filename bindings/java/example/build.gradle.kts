plugins {
    id("java")
}

group = "tech.turso"
version = "1.0-SNAPSHOT"

repositories {
    mavenLocal()
    mavenCentral()
}

dependencies {
    implementation("tech.turso:limbo:0.0.1-SNAPSHOT")
    testImplementation(platform("org.junit:junit-bom:5.10.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")
}

tasks.test {
    useJUnitPlatform()
}

tasks.register<JavaExec>("run") {
    group = "application"
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("tech.turso.Main")
}
