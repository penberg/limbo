plugins {
    java
    application
}

group = "org.github.tursodatabase"
version = "0.0.1-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    testImplementation(platform("org.junit:junit-bom:5.10.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("org.assertj:assertj-core:3.27.0")
}

application {
    mainClass.set("org.github.tursodatabase.Main")

    val limboSystemLibraryPath = System.getenv("LIMBO_SYSTEM_PATH")
    if (limboSystemLibraryPath != null) {
        applicationDefaultJvmArgs = listOf(
            "-Djava.library.path=${System.getProperty("java.library.path")}:$limboSystemLibraryPath"
        )
    }
}

tasks.test {
    useJUnitPlatform()
    // In order to find rust built file under resources, we need to set it as system path
    systemProperty("java.library.path", "${System.getProperty("java.library.path")}:$projectDir/src/test/resources/limbo/debug")
}
