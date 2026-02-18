#! /bin/bash

# Developer Notice: We have used the sdmetrics 0.25.0 vesion: https://github.com/sdv-dev/SDMetrics/tree/b0a8038f272962eec18fdb55ff0d83fbb522b9e0
# If reading this sounds old, you might want to
# change the SDMETRICS_COMMIT_SHA to a more recent one.
# You can find a suitable one here: https://github.com/sdv-dev/SDMetrics/commits/main/
# Having said that, we cannot ensure compatibility with or reproducibility of our source code in such a case.
# Therefore, if you wish to completely reproduce our results, we highly recommend you stick to the same commit SHA.
# If you wish to run a new investigation leveraging the latest sdmetrics code, feel free to update the commit SHA and give it a try.

# The patch fixes a bug where pd.concat of categorical columns with different category sets
# (e.g. synthetic data containing misspelled values) causes dtype fallback to object,
# which then crashes XGBoost's enable_categorical=True.

readonly SDMETRICS_COMMIT_SHA='b0a8038f272962eec18fdb55ff0d83fbb522b9e0'
readonly SDMETRICS_TEMP_DIRECTORY='sdmetrics-base-temp'

SCRIPT_DIR="$(dirname "$(readlink -f "$0")")" # resolves to <your-path-to-the-repo>/SynQTab/scripts/
PROJECT_ROOT_DIR="$(dirname "$SCRIPT_DIR")"   # resolves to <your-path-to-the-repo>/SynQTab/ which is the root of the project

echo "STEP 1 of 5: Cloning the base sdmetrics repository."
git clone https://github.com/sdv-dev/SDMetrics.git ${SDMETRICS_TEMP_DIRECTORY} &> /dev/null \
    && echo -e "└❯ ✅ Successfully cloned the sdmetrics repo!\n" || echo -e "└❯ ❌ ERROR: Cloning sdmetrics failed!\n"


echo "STEP 2 of 5: Travelling back in time to ensure reproducibility. Using commit SHA" ${SDMETRICS_COMMIT_SHA}
cd ${SDMETRICS_TEMP_DIRECTORY}
git checkout ${SDMETRICS_COMMIT_SHA} &> /dev/null \
    && echo -e "└❯ ✅ Successfully checked out to the commit SHA!\n" || echo -e "└❯ ❌ ERROR: Checking out the commit SHA failed!\n"


echo "STEP 3 of 5: Revamping sdmetrics"
git apply $SCRIPT_DIR/sdmetrics-patches/sdmetrics-concat-categorical.patch &> /dev/null \
    && echo -e "└❯ ✅ Successfully the patch for categorical concatenation!" || echo -e "└❯ ❌ ERROR: Applying the patch for categorical concatenation has failed!"


echo "STEP 4 of 5: Installing the revamped sdmetrics package"
# if you are not using uv, modify the following line accordingly, e.g., `pip install .` for pip-based package management
uv pip install . &> /dev/null \
    && echo -e "└❯ ✅ Successfully installed the revamped sdmetrics package!\n" || echo -e "└❯ ❌ ERROR: Installing the revamped sdmetrics package has failed!\n"


echo "STEP 5 of 5: Cleaning up"
cd ../ && rm -rf ${SDMETRICS_TEMP_DIRECTORY} \
    && echo -e "└❯ ✅ Successfully cleaned up the temporary directory!\n" || echo -e "└❯ ❌ ERROR: Cleaning up the temporary directory has failed!\n"


echo "========= INFO ========="
echo "Installed sdmetrics:" $(uv pip show sdmetrics | grep -i version) "+patch for categorical concatenation"

