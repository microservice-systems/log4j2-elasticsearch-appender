#!/bin/bash

mvn clean
mvn deploy -DaltDeploymentRepository=sonatype::default::https://oss.sonatype.org/service/local/staging/deploy/maven2 -DskipTests
