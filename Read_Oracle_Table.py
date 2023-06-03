from pyspark.sql import SparkSession
import cx_Oracle

spark = SparkSession.builder \
    .appName("Read Oracle Table") \
    .getOrCreate()

connection = cx_Oracle.connect("job_control/pr0duct1on@OGCDB-CH2-CP-GRID.SYS.COMCAST.NET:1555/JOBCTLP")

query = "SELECT * FROM (SELECT DATA_DATE,START_TIME,END_TIME,STATUS FROM JOB_CONTROL.MELD_JOB_STATUS WHERE PROGRAM_ID =(SELECT PROGRAM_ID FROM JOB_CONTROL.MELD_PROGRAM_NM WHERE lower(NAME) = '{0}')ORDER BY JOB_STATUS_ID desc FETCH FIRST 1 ROWS ONLY),(SELECT VARIABLE_VALUE FROM JOB_CONTROL.MELD_VARIABLES WHERE PROGRAM_ID=(SELECT PROGRAM_ID FROM JOB_CONTROL.MELD_PROGRAM_NM WHERE NAME = '{1}') AND VARIABLE_NAME='{2}')"

df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:oracle:thin:@OGCDB-CH2-CP-GRID.SYS.COMCAST.NET:1555/JOBCTLP") \
    .option("dbtable", f"({query})") \
    .option("user", "job_control") \
    .option("password", "pr0duct1on") \
    .option("driver", "oracle.jdbc.driver.OracleDriver") \
    .load()

df.show()
