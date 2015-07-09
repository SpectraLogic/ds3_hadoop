# DS3 Hadoop SDK

---

A Hadoop Map/Reduce library that migrates data between a Hadoop Cluster and a DS3 appliance.


## Contact Us

Join us at our [Google Groups](https://groups.google.com/d/forum/spectralogicds3-sdks) forum to ask questions, or see frequently asked questions.

## Javadoc

The most recent javadoc can be accessed here: [Javadoc 0.8.1](http://spectralogic.github.io/ds3_hadoop/javadoc/v0.8.1/)

## CLI

Please see the [CLI docs](ds3-hadoop-cli/README.md) for more information on how to use the Spectra S3 Hadoop CLI.

## Installing

To install the latest ds3_hadoop code either download the latest release jar file from the [Releases](../../releases) or use the DS3 Hadoop SDK code from our pre-compiled Jars in your Maven or Gradle builds in the following way.

```xml

<project>
  ...
  <repositories>
    <repository>
      <id>Spectra</id>
      <url>http://dl.bintray.com/spectralogic/ds3</url>
    </repository>
  </repositories>
  ...
    <dependencies>
      ...
      <dependency>
        <groupId>com.spectralogic.ds3-hadoop</groupId>
        <artifactId>ds3-hadoop-core</artifactId>
        <version>0.8.1</version>
      </dependency>
    ...  
    </dependencies>
</project>

```

To include the sdk into Gradle include the following in the `build.gradle` file:

```groovy
apply plugin: 'maven'

repositories {
    ...
    maven {
        url 'http://dl.bintray.com/spectralogic/ds3'
    }
    ...
}

dependencies {
    ...
    compile 'com.spectralogic.ds3-hadoop:ds3-hadoop-core:0.8.1'
    ...
}

```

Releases
========

Version `0.8.1` is available for use.  Only bulk puts are supported with this release.

Samples
=======

The repository includes a module that contains sample programs that use the ds3-hadoop API.  They can be referenced [here](https://github.com/SpectraLogic/ds3_hadoop/tree/master/ds3-hadoop-samples/src/main/java/com/spectralogic/hadoop/sample)
 
To run the examples, make sure the one you want to run is uncommented in [build.gradle](https://github.com/SpectraLogic/ds3_hadoop/tree/master/ds3-hadoop-samples/build.gradle) in the `ds3-hadoop-samples` module.  Then from the project root run: `./gradlew ds3-hadoop-samples:run` which will compile and run the sample.
