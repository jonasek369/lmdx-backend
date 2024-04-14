plugins {
    kotlin("jvm") version "1.9.21"
    kotlin("plugin.serialization") version "1.9.22"
}

group = "me.jonas"
version = "1.0-SNAPSHOT"
val ktorVersion: String by project
val exposedVersion: String by project



repositories {
    google()
    mavenCentral()
}

dependencies {
    testImplementation("org.jetbrains.kotlin:kotlin-test")

    implementation("io.ktor:ktor-client-core:$ktorVersion")
    implementation("io.ktor:ktor-client-cio:$ktorVersion")
    implementation("io.ktor:ktor-serialization-kotlinx-json:$ktorVersion")
    implementation("io.ktor:ktor-client-content-negotiation:$ktorVersion")
    implementation("io.ktor:ktor-client-android:$ktorVersion")
    implementation("io.ktor:ktor-client-serialization:$ktorVersion")
    implementation("io.ktor:ktor-client-json:$ktorVersion")
    implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:LATEST")

    implementation("org.jetbrains.exposed:exposed-core:$exposedVersion")
    implementation("org.jetbrains.exposed:exposed-crypt:$exposedVersion")
    implementation("org.jetbrains.exposed:exposed-dao:$exposedVersion")
    implementation("org.jetbrains.exposed:exposed-jdbc:$exposedVersion")

    implementation("org.jetbrains.exposed:exposed-jodatime:$exposedVersion")
    // or
    implementation("org.jetbrains.exposed:exposed-java-time:$exposedVersion")
    // or
    implementation("org.jetbrains.exposed:exposed-kotlin-datetime:$exposedVersion")

    implementation("org.jetbrains.exposed:exposed-json:$exposedVersion")
    implementation("org.jetbrains.exposed:exposed-money:$exposedVersion")

    implementation("org.xerial:sqlite-jdbc:3.44.1.0")

}

tasks.test {
    useJUnitPlatform()
}
kotlin {
    jvmToolchain(17)
}