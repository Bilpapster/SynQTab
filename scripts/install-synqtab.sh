#! /bin/bash

SCRIPT_DIR="$(dirname "$(readlink -f "$0")")" # resolves to <your-path-to-the-repo>/SynQTab/scripts/
PROJECT_ROOT_DIR="$(dirname "$SCRIPT_DIR")"   # resolves to <your-path-to-the-repo>/SynQTab/ which is the root of the project

uv pip install --no-cache --quiet -e ${PROJECT_ROOT_DIR}

echo "========= INFO ========="
echo "Installed synqtab:" $(uv pip show synqtab | grep -i version)
