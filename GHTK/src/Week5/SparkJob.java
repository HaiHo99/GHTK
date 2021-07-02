package Week5;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class SparkJob {
    public static void main(String[] args) {

        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();
        Dataset<Row> df = spark.read().parquet("/user/haiht34/Sample_data");
        df.createOrReplaceTempView("transaction");
        df = spark.sql("select * from transaction where device_model is not null");
        df.createOrReplaceTempView("transaction1");
        Dataset<Row> device_model_num_user = spark.sql("SELECT device_model, count(user_id) as count FROM transaction1 GROUP BY device_model");
        Dataset<Row> device_model_list_user = spark.sql("SELECT device_model, concat_ws(',',collect_set(user_id)) as list_user_id FROM transaction1 GROUP BY device_model");
        Dataset<Row> action_by_button_id = spark.sql("select concat(user_id, '_', device_model) as user_id_device_model, button_id, count(*) as action_by_button_id from transaction1 where button_id is not null group by user_id_device_model, button_id");
        device_model_num_user.repartition(1).write().mode(SaveMode.Overwrite).option("compression", "snappy").parquet("hdfs://10.140.0.13:9000/user/haiht34/device_model_num_user");
        device_model_list_user.repartition(1).write().mode(SaveMode.Overwrite).option("compression", "snappy").orc("hdfs://10.140.0.13:9000/user/haiht34/device_model_list_user");
        action_by_button_id.repartition(1).write().mode(SaveMode.Overwrite).option("compression", "snappy").parquet("hdfs://10.140.0.13:9000/user/haiht34/button_count_by_user_id_device_model");
    }
}