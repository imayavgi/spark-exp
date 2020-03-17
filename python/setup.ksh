#SPARK SETUP
export JAVA_HOME=`/usr/libexec/java_home -v 1.8.0_202`

export SPARK_HOME=/Users/imayakulothungan/devtools/spark
export PATH="$SPARK_HOME/bin:$PATH"

export PYSPARK_SUBMIT_ARGS="pyspark-shell"
export PYSPARK_DRIVER_PYTHON=ipython
export PYSPARK_DRIVER_PYTHON_OPTS='notebook' pyspark
