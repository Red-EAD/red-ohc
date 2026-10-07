#!/usr/bin/env bash

set -euo pipefail

REPO_ROOT=$(cd "$(dirname "$0")/.." && pwd)
OUTPUT_DIR=${1:-"$REPO_ROOT/red-ohc-core/target/jcstress"}
JAVA_BIN=${JAVA_BIN:-}
JAVAC_BIN=${JAVAC_BIN:-}
JAR_BIN=${JAR_BIN:-}
MAVEN_BIN=${MAVEN_BIN:-"$REPO_ROOT/mvnw"}
ITERATIONS=${RED_OHC_JCSTRESS_ITERATIONS:-1}
MODE=${RED_OHC_JCSTRESS_MODE:-sanity}
CPUS=${RED_OHC_JCSTRESS_CPUS:-2}
TIME=${RED_OHC_JCSTRESS_TIME:-100}
TEST_SELECTOR=${RED_OHC_JCSTRESS_TEST_SELECTOR:-'com\.red\.ohc\..*Stress'}

if [ -z "$JAVA_BIN" ]; then
  JAVA_BIN=${JAVA_HOME:+$JAVA_HOME/bin/java}
  JAVA_BIN=${JAVA_BIN:-$(command -v java || true)}
fi
if [ -z "$JAVAC_BIN" ]; then
  JAVAC_BIN=${JAVA_HOME:+$JAVA_HOME/bin/javac}
  JAVAC_BIN=${JAVAC_BIN:-$(command -v javac || true)}
fi
if [ -z "$JAR_BIN" ]; then
  JAR_BIN=${JAVA_HOME:+$JAVA_HOME/bin/jar}
  JAR_BIN=${JAR_BIN:-$(command -v jar || true)}
fi

if [ ! -x "$JAVA_BIN" ] || [ ! -x "$JAVAC_BIN" ] || [ ! -x "$JAR_BIN" ]; then
  echo "JDK is required; set JAVA_HOME or JAVA_BIN/JAVAC_BIN/JAR_BIN" >&2
  exit 2
fi
case "$ITERATIONS" in
  ''|*[!0-9]*|0) echo "RED_OHC_JCSTRESS_ITERATIONS must be positive" >&2; exit 2 ;;
esac
case "$CPUS" in
  ''|*[!0-9]*|0) echo "RED_OHC_JCSTRESS_CPUS must be positive" >&2; exit 2 ;;
esac

mkdir -p "$OUTPUT_DIR"
OUTPUT_DIR=$(cd "$OUTPUT_DIR" && pwd)
mkdir -p "$OUTPUT_DIR/classes" "$OUTPUT_DIR/report"
CP_FILE="$OUTPUT_DIR/maven-classpath.txt"
cd "$REPO_ROOT"
"$MAVEN_BIN" -q -pl red-ohc-core -am -DskipTests compile org.apache.maven.plugins:maven-dependency-plugin:3.8.1:build-classpath \
  -DincludeScope=test \
  -Dmdep.outputFile="$CP_FILE"
MAVEN_CP=$(tr -d '\n' < "$CP_FILE")
CORE_CLASSES="$REPO_ROOT/red-ohc-core/target/classes"
SOURCE_DIR="$REPO_ROOT/red-ohc-core/src/jcstress/java"

if [ ! -d "$CORE_CLASSES" ]; then
  echo "missing $CORE_CLASSES; build red-ohc-core before running jcstress" >&2
  exit 2
fi
if [ -z "$MAVEN_CP" ]; then
  echo "Maven did not return a test classpath" >&2
  exit 2
fi

SOURCES=()
while IFS= read -r source; do
  SOURCES+=("$source")
done < <(find "$SOURCE_DIR" -type f -name '*.java' | sort)
if [ "${#SOURCES[@]}" -eq 0 ]; then
  echo "no jcstress sources found under $SOURCE_DIR" >&2
  exit 2
fi

rm -rf "$OUTPUT_DIR/classes"
mkdir -p "$OUTPUT_DIR/classes"
"$JAVAC_BIN" \
  --release 11 \
  -cp "$CORE_CLASSES:$MAVEN_CP" \
  -processorpath "$MAVEN_CP" \
  -processor org.openjdk.jcstress.infra.processors.JCStressTestProcessor \
  -d "$OUTPUT_DIR/classes" \
  "${SOURCES[@]}"

CORE_JAR="$OUTPUT_DIR/red-ohc-core-classes.jar"
TEST_JAR="$OUTPUT_DIR/jcstress-tests.jar"
"$JAR_BIN" cf "$CORE_JAR" -C "$CORE_CLASSES" .
"$JAR_BIN" cf "$TEST_JAR" -C "$OUTPUT_DIR/classes" .

cd "$OUTPUT_DIR"
exec "$JAVA_BIN" \
  -cp "$TEST_JAR:$CORE_JAR:$MAVEN_CP" \
  org.openjdk.jcstress.Main \
  -t "$TEST_SELECTOR" \
  -m "$MODE" \
  -iters "$ITERATIONS" \
  -time "$TIME" \
  -c "$CPUS" \
  -r "$OUTPUT_DIR/report"
