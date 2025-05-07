echo "Start copy dags to the airflow dags folder"

# Locate the Airflow executable
# AIRFLOW_EXECUTABLE=$(which airflow)
# AIRFLOW_DIR=$(dirname "$AIRFLOW_EXECUTABLE")
REPO_NAME=$(basename -s .git $(git config --get remote.origin.url))
DESTINATION_DIR="$AIRFLOW_HOME/dags/$REPO_NAME"

# echo "Airflow executable: $AIRFLOW_EXECUTABLE"
# echo "Airflow directory: $AIRFLOW_DIR"
echo "Destination directory: $DESTINATION_DIR"

# Create the destination directory if not exists
mkdir -p "$DESTINATION_DIR"
cp -r ./pipeline/dags/* $DESTINATION_DIR

ls $DESTINATION_DIR
