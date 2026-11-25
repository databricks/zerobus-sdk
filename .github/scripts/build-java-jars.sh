#!/usr/bin/env bash
set -euo pipefail

# Builds the Java SDK JARs into java/dist. Run from the java directory; extra arguments go to Maven.
#
# The fat JAR bundles protobuf-java 4.33.0, so it is built with the default runtime. The regular
# JAR is compiled against protobuf-java 3.25.9, because classes compiled against 4.33.0 carry a
# bridge method that fails verification on 3.25.x. Maven resolves one version per artifact, so the
# two JARs need two builds.

rm -rf dist
mkdir dist

mvn --batch-mode -DskipTests=true clean package "$@"
cp target/zerobus-ingest-sdk-*.jar dist/

mvn --batch-mode -DskipTests=true clean package -Dprotobuf.version=3.25.9 "$@"
for jar in target/zerobus-ingest-sdk-*.jar; do
  case "$jar" in
    *-jar-with-dependencies.jar|*-sources.jar|*-javadoc.jar) ;;
    *) cp "$jar" dist/ ;;
  esac
done

ls -l dist
