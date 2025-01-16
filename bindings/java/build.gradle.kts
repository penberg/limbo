import net.ltgt.gradle.errorprone.CheckSeverity
import net.ltgt.gradle.errorprone.errorprone

plugins {
    java
    application
    id("net.ltgt.errorprone") version "3.1.0"
}

group = "org.github.tursodatabase"
version = "0.0.1-SNAPSHOT"

java {
    sourceCompatibility = JavaVersion.VERSION_1_8
    targetCompatibility = JavaVersion.VERSION_1_8
}

repositories {
    mavenCentral()
}

dependencies {
    implementation("ch.qos.logback:logback-classic:1.5.16")
    implementation("ch.qos.logback:logback-core:1.5.16")

    errorprone("com.uber.nullaway:nullaway:0.10.26") // maximum version which supports java 8
    errorprone("com.google.errorprone:error_prone_core:2.10.0") // maximum version which supports java 8

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
    systemProperty(
        "java.library.path",
        "${System.getProperty("java.library.path")}:$projectDir/src/test/resources/limbo/debug"
    )
}

tasks.withType<JavaCompile> {
    options.errorprone {
        // Let's select which checks to perform. NullAway is enough for now.
        disableAllChecks = true
        check("NullAway", CheckSeverity.ERROR)

        option("NullAway:AnnotatedPackages", "org.github.tursodatabase")
        option(
            "NullAway:CustomNullableAnnotations",
            "org.github.tursodatabase.annotations.Nullable,org.github.tursodatabase.annotations.SkipNullableCheck"
        )
    }
    if (name.lowercase().contains("test")) {
        options.errorprone {
            disable("NullAway")
        }
    }
}
