set -euxo pipefail
# export TEST_NAME="${APP_NAME}_${USER}_test"

# Build gcloud dataproc job submission command
gcloud_command="gcloud dataproc jobs submit spark"

# Set arguments based on environment variables
gcloud_command+=" --cluster=${CLUSTER_NAME:-chronon-cluster}"  # Dataproc cluster name (required)
gcloud_command+=" --region=${REGION:-us-central1}"        # Dataproc cluster region (required)

# Map Spark arguments to Dataproc arguments
gcloud_command+=" --jars=${CHRONON_ONLINE_JAR:-/Users/feltorr/Projects/chronon/spark/target/scala-2.12/spark_uber-assembly-0.0.79-SNAPSHOT.jar}"

# Additional Dataproc arguments (optional)
gcloud_command+=" --properties="  # Set Dataproc cluster properties (key=value pairs)
gcloud_command+="spark.chronon.partition.column=${PARTITION_COLUMN:-ds},"
gcloud_command+="spark.chronon.partition.format=${PARTITION_FORMAT:-yyyy-MM-dd},"
gcloud_command+="spark.chronon.backfill.validation.enabled=${ENABLE_VALIDATION:-false},"
gcloud_command+="spark.app.name=${APP_NAME:-chronon-app-name},"
gcloud_command+="spark.chronon.outputParallelismOverride=${OUTPUT_PARALLELISM:--1},"
gcloud_command+="spark.chronon.rowCountPerPartition=${ROW_COUNT_PER_PARTITION:--1},"
gcloud_command+="spark.chronon.sql.format=bigquery,"
gcloud_command+="materializationDataset=${MATERIALIZATION_DATASET:-chronon_tmp},"
gcloud_command+="viewsEnabled=${VIEWS_ENABLED:-true}"

# Arguments passed to the Spark application
gcloud_command+=" --class ai.chronon.spark.Driver"
gcloud_command+=" -- staging-query-backfill"
gcloud_command+=" --conf-path=production/staging_queries/kaggle/dim_client.base_table"

gcloud_command+=" --end-date=2024-08-09"

# Submit the job
$gcloud_command 2>&1
