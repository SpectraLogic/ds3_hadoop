# DS3 Hadoop SDK

---

A Hadoop Map/Reduce library that migrates data between a Hadoop Cluster and a DS3 appliance.

## Installing

To install the latest ds3_hadoop code either download the latest release jar file from the [Releases](../../releases) page or clone the repository and run `./gradlew install` to install the sdk into your local maven repository.

You can also use the ds3_hadoop code with our pre-compiled Jars in your Maven or Gradle builds in the following way.

```xml

<project>
  ...
  <repositories>
    <repository>
      <id>Spectra-Github</id>
      <url>https://spectralogic.github.io/java/repository</url>
    </repository>
  </repositories>
  ...
    <dependencies>
      ...
      <dependency>
        <groupId>com.spectralogic.ds3-hadoop</groupId>
        <artifactId>ds3-hadoop-core</artifactId>
        <version>0.7.0-STUB</version>
      </dependency>
    ...  
    </dependencies>
</project>

```

To include the sdk into Gradle include the following in the `build.gradle` file:

```groovy

repositories {
    ...
    maven {
        url 'https://spectralogic.github.io/java/repository'
    }
    ...
}

dependencies {
    ...
    compile 'com.spectralogic.ds3-hadoop:ds3-hadoop-core:0.7.0-STUB'
    ...
}

```

Releases
========

Currently the only release available is the STUB release.  This release contains the full API, but with a stubbed out implementation.  This allows for a user to begin writing an application with the API, but without the need for a fully functional Hadoop Cluster.

Samples
=======

The repository includes a module that contains sample programs that use the ds3-hadoop API.  They can be referenced [here](https://github.com/SpectraLogic/ds3_hadoop/tree/master/ds3-hadoop-samples/src/main/java/com/spectralogic/hadoop/sample)
