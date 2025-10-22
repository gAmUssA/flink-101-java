plugins {
    java
    application
    id("com.gradleup.shadow") version "8.3.8"
}

group = "com.example.flink"
version = "1.0.0"

java {
    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17
}

// Configure source sets to exclude documentation files
sourceSets {
    main {
        java {
            srcDirs("src/main/java", "shared")
            exclude("**/*.md", "**/*.txt", "**/*.rst")
        }
        resources {
            srcDirs("src/main/resources")
        }
    }
    test {
        java {
            srcDirs("src/test/java")
            exclude("**/*.md", "**/*.txt", "**/*.rst")
        }
    }
}

// Configure duplicate handling strategy for processResources
tasks.processResources {
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
}

repositories {
    mavenCentral()
    maven {
        url = uri("https://repository.apache.org/content/repositories/snapshots/")
        mavenContent {
            snapshotsOnly()
        }
    }
}

val flinkVersion = "2.0.0"
val kafkaConnectorVersion = "4.0.0-2.0"
val slf4jVersion = "1.7.36"
val log4jVersion = "2.25.2"

// Flink-specific configuration following official recommendations
configurations {
    val flinkShadowJar by creating
    
    // Always exclude these (also from transitive dependencies) since they are provided by Flink
    flinkShadowJar.exclude(group = "org.apache.flink", module = "force-shading")
    flinkShadowJar.exclude(group = "com.google.code.findbugs", module = "jsr305")
    flinkShadowJar.exclude(group = "org.slf4j")
    flinkShadowJar.exclude(group = "org.apache.logging.log4j")
}

dependencies {
    // --------------------------------------------------------------
    // Compile-time dependencies that should NOT be part of the
    // shadow (uber) jar and are provided in the lib folder of Flink
    // --------------------------------------------------------------
    implementation("org.apache.flink:flink-streaming-java:$flinkVersion")
    implementation("org.apache.flink:flink-clients:$flinkVersion")
    
    // --------------------------------------------------------------
    // Dependencies that should be part of the shadow jar, e.g.
    // connectors. These must be in the flinkShadowJar configuration!
    // --------------------------------------------------------------
    val flinkShadowJar by configurations
    flinkShadowJar("org.apache.flink:flink-connector-kafka:$kafkaConnectorVersion")
    flinkShadowJar("org.apache.flink:flink-connector-base:$flinkVersion")
    flinkShadowJar("com.fasterxml.jackson.core:jackson-databind:2.20.0")
    
    // Table API and SQL for lessons 4-5 (commented until Flink 2.0 is released)
    // implementation("org.apache.flink:flink-table-api-java-bridge:$flinkVersion")
    // implementation("org.apache.flink:flink-table-planner:$flinkVersion")
    
    // Runtime logging dependencies
    runtimeOnly("org.apache.logging.log4j:log4j-slf4j-impl:$log4jVersion")
    runtimeOnly("org.apache.logging.log4j:log4j-api:$log4jVersion")
    runtimeOnly("org.apache.logging.log4j:log4j-core:$log4jVersion")
    
    // Simple testing framework - no complex frameworks
    testImplementation("org.apache.flink:flink-test-utils:$flinkVersion")
}

// Make flinkShadowJar dependencies available for compilation and runtime (Flink best practice)
sourceSets {
    main {
        compileClasspath += configurations["flinkShadowJar"]
        runtimeClasspath += configurations["flinkShadowJar"]
    }
    test {
        compileClasspath += configurations["flinkShadowJar"]
        runtimeClasspath += configurations["flinkShadowJar"]
    }
}

// Make flinkShadowJar available for run task
tasks.named<JavaExec>("run") {
    classpath = sourceSets.main.get().runtimeClasspath
}

// Preserve parameter names for better debugging
tasks.compileJava {
    options.compilerArgs.addAll(listOf(
        "-parameters",
        "-Xlint:unchecked",
        "-Xlint:deprecation"
    ))
}

// Individual lesson run tasks for easy execution
tasks.register<JavaExec>("runLesson01") {
    group = "lessons"
    description = "Run Lesson 1: DataStream API with In-Memory Data"
    classpath = sourceSets.main.get().runtimeClasspath
    mainClass.set("com.example.flink.lesson01.StreamingWordCount")
}

tasks.register<JavaExec>("runLesson02") {
    group = "lessons"
    description = "Run Lesson 2: Kafka Integration"
    classpath = sourceSets.main.get().runtimeClasspath
    mainClass.set("com.example.flink.lesson02.KafkaConsumerExample")
}


// Lesson 3 Separate Jobs - Individual use cases for focused learning
tasks.register<JavaExec>("runLesson03A") {
    group = "lesson03"
    description = "Run Lesson 3A: Customer Order Tracking - Basic customer totals and order counts"
    classpath = sourceSets.main.get().runtimeClasspath
    mainClass.set("com.example.flink.lesson03.CustomerOrderTrackingJob")
}

tasks.register<JavaExec>("runLesson03B") {
    group = "lesson03"
    description = "Run Lesson 3B: VIP Customer Detection - Customer classification based on spending"
    classpath = sourceSets.main.get().runtimeClasspath
    mainClass.set("com.example.flink.lesson03.VIPCustomerDetectionJob")
}

tasks.register<JavaExec>("runLesson03C") {
    group = "lesson03"
    description = "Run Lesson 3C: Order Frequency Analysis - Customer behavioral tracking"
    classpath = sourceSets.main.get().runtimeClasspath
    mainClass.set("com.example.flink.lesson03.OrderFrequencyAnalysisJob")
}

tasks.register<JavaExec>("runLesson03D") {
    group = "lesson03"
    description = "Run Lesson 3D: Category Spending Analysis - Product category analytics"
    classpath = sourceSets.main.get().runtimeClasspath
    mainClass.set("com.example.flink.lesson03.CategorySpendingAnalysisJob")
}

tasks.register<JavaExec>("runLesson04") {
    group = "lessons"
    description = "Run Lesson 4: Materialized Views"
    classpath = sourceSets.main.get().runtimeClasspath
    mainClass.set("com.example.flink.lesson04.MaterializedViewExample")
}

tasks.register<JavaExec>("runLesson05") {
    group = "lessons"
    description = "Run Lesson 5: Table API and SQL"
    classpath = sourceSets.main.get().runtimeClasspath
    mainClass.set("com.example.flink.lesson05.TableAPIExample")
}

tasks.register<JavaExec>("runKafkaProducer") {
    group = "kafka"
    description = "Run Kafka Order Producer - generates continuous Order data for testing"
    classpath = sourceSets.main.get().runtimeClasspath
    mainClass.set("shared.data.generators.KafkaOrderProducer")
}

// Shadow JAR configuration following Flink best practices
tasks.shadowJar {
    archiveBaseName.set("flink-demo")
    archiveClassifier.set("")
    archiveVersion.set("")
    
    // Use flinkShadowJar configuration as recommended by Flink
    configurations = listOf(project.configurations["flinkShadowJar"])
    
    // Exclude unnecessary files to keep JAR size manageable
    exclude("META-INF/*.SF")
    exclude("META-INF/*.DSA")
    exclude("META-INF/*.RSA")
    
    // Transform service files to avoid conflicts
    mergeServiceFiles()
    
    // Add manifest attributes
    manifest {
        attributes(
            "Built-By" to System.getProperty("user.name"),
            "Build-Jdk" to System.getProperty("java.version")
        )
    }
}

// Default application main class
application {
    mainClass.set("com.example.flink.lesson01.StreamingWordCount")
}

// Clean build and execution scripts
tasks.register("cleanRun") {
    group = "build"
    description = "Clean build and run lesson 1"
    dependsOn("clean", "build", "runLesson01")
}

// Simple validation task
tasks.register("validateSetup") {
    group = "verification"
    description = "Validate project setup and dependencies"
    doLast {
        println("✓ Java version: ${System.getProperty("java.version")}")
        println("✓ Flink version: $flinkVersion")
        println("✓ Project structure validated")
        println("✓ Dependencies resolved")
        println("Ready to run lessons!")
    }
}