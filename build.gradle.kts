plugins {
    `java-library`
    `maven-publish`
}

group = providers.gradleProperty("GROUP_NAME").get()
version = providers.gradleProperty("VERSION_NAME").get()

repositories {
    mavenCentral()
}

dependencies {
    annotationProcessor(rootProject.libs.org.projectlombok)
    implementation(rootProject.libs.org.projectlombok)
    implementation(rootProject.libs.org.slf4j.api)
    implementation(rootProject.libs.com.h2database.mvstore)

    testImplementation(rootProject.libs.com.google.guava)
    testImplementation(platform(rootProject.libs.org.junit.bom))
    testImplementation(rootProject.libs.org.junit.jupiter)
    testImplementation(rootProject.libs.org.assertj.core)
    testImplementation(rootProject.libs.org.awaitility)
    testRuntimeOnly(rootProject.libs.org.junit.platform)
}

tasks.test {
    useJUnitPlatform()
}

java {
    sourceCompatibility = JavaVersion.VERSION_1_8
    targetCompatibility = JavaVersion.VERSION_1_8
}

publishing {
    repositories {
        maven {
            name = "GitHubPackages"
            url = uri("https://maven.pkg.github.com/OWNER/REPOSITORY")
            credentials {
                username = project.findProperty("gpr.user") as String? ?: System.getenv("USERNAME")
                password = project.findProperty("gpr.key") as String? ?: System.getenv("TOKEN")
            }
        }
    }

    publications {
        register<MavenPublication>("gpr") {
            from(components["java"])
        }
    }
}