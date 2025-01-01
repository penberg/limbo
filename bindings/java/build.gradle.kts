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
}
