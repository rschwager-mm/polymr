test -z "${PYTHON_HOME}" && { echo "no PYTHON_HOME found"; exit 1; }

"$PYTHON_HOME/bin/python" -m venv venv/
source venv/bin/activate

pip install -U pip
pip install .

# This deactivate is needed, otherwise we get errors from
# /etc/hadoop/conf.cloudera.yarn/topology.py
deactivate

# Package up an isolated environment for our job.
pushd venv/
zip -rq ../venv.zip *
popd
pushd polymr/
zip -rq ../splinkr.zip *
popd

export PYSPARK_PYTHON="venv/bin/python"
export JAVA_HOME="/usr/java/jdk1.7.0_80/"
export SPARK_HOME="/hadoop/spark/2.0"
export PATH="$SPARK_HOME/bin:$PATH"
export HADOOP_CONF_DIR="/etc/hadoop/conf"
export SPARK_DIST_CLASSPATH=$(hadoop classpath)

spark-submit \
  --name "polymr_index" \
  --master yarn \
  --deploy-mode client \
  --conf "spark.yarn.appMasterEnv.PYSPARK_PYTHON=$PYSPARK_PYTHON" \
  --conf "spark.yarn.appMasterEnv.SPARK_HOME=$SPARK_HOME" \
  --archives "venv.zip#venv,splinkr.zip#splinkr" \
  --driver-memory 4G \
  --executor-memory 12G \
  --driver-cores 1 \
  --executor-cores 4 \
  --conf "spark.yarn.executor.memoryOverhead=3000" \
  --conf "spark.ui.showConsoleProgress=false" \
  spark/spark_script.py "$@"
