#!/bin/bash
type realpath &>/dev/null
if [ $? -ne 0 ]; then
    function realpath() {
        [[ $1 = /* ]] && echo "$1" || echo "$PWD/${1#./}"
    }
fi

: "${RUN_PATH:="$(realpath "$(dirname "${BASH_SOURCE[0]}")")"}"
: "${IMAGE_TAG:="airflow_kubernetes_job_operator_example_image:latest"}"
: "${DAGS_PATH:="$(realpath "$RUN_PATH/../../tests/dags")"}"
: "${DATA_PATH:="$(realpath "$RUN_PATH/../../.local/kube/data")"}"
