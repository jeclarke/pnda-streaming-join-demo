# join-demo

## Overview

This application joins an input stream to a static / slowly changing topolgy dataset for context.

## Requirements

* [Maven](https://maven.apache.org/docs/3.0.5/release-notes.html) 3.0.5
* [Java JDK](https://docs.oracle.com/javase/8/docs/technotes/guides/install/install_overview.html) 1.8

## Build

Please note this currently only builds on OSX or Linux until package-py-deps.sh can be modified to run in a cross-platform way.

To build this example application use:

```
mvn clean package
```

The output file will be created as:

```
app-package/target/join-demo-0.0.1.tar.gz
```
