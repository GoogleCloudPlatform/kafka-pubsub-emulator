#!/usr/bin/env bash
#  Copyright 2018 Google LLC
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

# Primary build script

set -eu -o pipefail

GCP_PROJECT=cpe-ti
PROJECT_NAME=kafka-pubsub-emulator

build() {
    # Cleans, tests, packages
    # $1 - GCS bucket path to publish the jar to
    # $2 - Maven goal - test/verify
    mvn -B clean $2 javadoc:javadoc package
    [[ $? -ne 0 ]] && return 1

    local jar="$(ls target/kafka-pubsub-emulator*.jar)"
    gsutil -m rm "gs://$1/*.jar" || echo "No files found in gs://$1";
    gsutil -h "Cache-Control: no-cache, no-store, must-revalidate" cp -v "$jar" "gs://$1/"
}

main() {
    if [[ -z "${GCS_FOLDER:=}" ]] ; then
        GCS_FOLDER="${1:-}"
    fi;
    [[ -z "${GCS_FOLDER}" ]] && \
        echo "ERROR: GCS_FOLDER environment variable or argument must be provided" \
        && exit 1

    local artifacts_bucket="${GCP_PROJECT}/${PROJECT_NAME}/${GCS_FOLDER}"
    echo "==== Initiating Build Process ===="
    local test_goal=test
    if [[ "$GCS_FOLDER" != "presubmit" ]] ; then
       echo "==== Will run integration tests ===="
       test_goal=verify
    fi

    if build "$artifacts_bucket" "$test_goal" ; then
      echo "==== Build Completed Successfully ===="
    else
      echo "==== Build Failed ===="
      exit 1
    fi

    return 0
}

main $*