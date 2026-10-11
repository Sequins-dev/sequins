plugins {
    `java-library`
    `maven-publish`
}

group = "dev.sequins"
version = "0.1.0"

java {
    sourceCompatibility = JavaVersion.VERSION_11
    targetCompatibility = JavaVersion.VERSION_11
    withJavadocJar()
    withSourcesJar()
}

repositories {
    mavenCentral()
}

val otelVersion = "1.44.1"
val otelAlphaVersion = "1.44.1-alpha"

dependencies {
    api("io.opentelemetry:opentelemetry-api:$otelVersion")
    api("io.opentelemetry:opentelemetry-sdk:$otelVersion")
    implementation("io.opentelemetry:opentelemetry-exporter-otlp:$otelVersion")
    implementation("io.opentelemetry:opentelemetry-sdk-extension-autoconfigure:$otelAlphaVersion")
    implementation("io.opentelemetry:opentelemetry-semconv:1.28.0-alpha")

    testImplementation("io.opentelemetry:opentelemetry-sdk-testing:$otelVersion")
    testImplementation(platform("org.junit:junit-bom:5.11.3"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

tasks.test {
    useJUnitPlatform()
}

publishing {
    publications {
        create<MavenPublication>("maven") {
            from(components["java"])
            pom {
                name.set("Sequins OpenTelemetry SDK")
                description.set("OpenTelemetry distro for Sequins — zero-config observability for local development")
                url.set("https://sequins.dev")
                licenses {
                    license {
                        name.set("MIT License")
                        url.set("https://opensource.org/licenses/MIT")
                    }
                }
            }
        }
    }
}
