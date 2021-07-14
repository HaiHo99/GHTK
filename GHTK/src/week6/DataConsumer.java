package week6;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import java.text.SimpleDateFormat;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.concurrent.TimeoutException;

public class DataConsumer {
    public static Timestamp getTime(long time){
        Timestamp rand = new Timestamp(time);
        return rand;
    }
    public static void main(String[] args) throws TimeoutException, IOException, StreamingQueryException {
        SimpleDateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
        SparkSession session = SparkSession.builder()
                .appName("SparkKafka")
                .getOrCreate();
        session.sparkContext().setLogLevel("ERROR");
        session.streams().awaitAnyTermination(1000);
        Dataset<Row> df = session.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers","10.140.0.13:9092")
                .option("subscribe","data_tracking_haiht34")
                .option("group.id","group10")
                .option("startingOffsets", "earliest")
                .option("auto.offset.reset","true")
                .option("value.serializer","org.apache.kafka.common.serialization.ByteArrayDeserializer")
                .load();
        Dataset<byte[]>words = df.select("value").as(Encoders.BINARY());
        Dataset<String> object = words.map((MapFunction<byte[], String>)
                s-> (DataTrack.DataTracking.parseFrom(s).getVersion()+
                        "#"+ DataTrack.DataTracking.parseFrom(s).getName()+
                        "#"+ dateFormat.format(DataTrack.DataTracking.parseFrom(s).getTimestamp())+
                        "#"+ DataTrack.DataTracking.parseFrom(s).getPhoneId()+
                        "#"+ DataTrack.DataTracking.parseFrom(s).getLon()+
                        "#"+ DataTrack.DataTracking.parseFrom(s).getLat()+
                        "#"+ (getTime(DataTrack.DataTracking.parseFrom(s).getTimestamp()).getYear()+1900)+
                        "#"+ (getTime(DataTrack.DataTracking.parseFrom(s).getTimestamp()).getMonth()+1)+
                        "#"+ getTime(DataTrack.DataTracking.parseFrom(s).getTimestamp()).getDate()+
                        "#"+ getTime(DataTrack.DataTracking.parseFrom(s).getTimestamp()).getHours()
                ),Encoders.STRING());

        Dataset<Row> result = object
                .withColumn("version", functions.split(object.col("value"), "#").getItem(0))
                .withColumn("name", functions.split(object.col("value"), "#").getItem(1))
                .withColumn("timestamp", functions.split(object.col("value"), "#").getItem(2))
                .withColumn("phone_id", functions.split(object.col("value"), "#").getItem(3))
                .withColumn("lon", functions.split(object.col("value"), "#").getItem(4))
                .withColumn("lat", functions.split(object.col("value"), "#").getItem(5))
                .withColumn("year", functions.split(object.col("value"), "#").getItem(6))
                .withColumn("month", functions.split(object.col("value"), "#").getItem(7))
                .withColumn("day", functions.split(object.col("value"), "#").getItem(8))
                .withColumn("hour", functions.split(object.col("value"), "#").getItem(9))
                .drop("value");
        StreamingQuery query = result
                .selectExpr("CAST(version AS STRING)","CAST(name AS STRING)","CAST(timestamp AS STRING)","CAST(phone_id AS STRING)","CAST(lon AS STRING)","CAST(lat AS STRING)","CAST(year AS STRING)","CAST(month AS STRING)","CAST(day AS STRING)","CAST(hour AS STRING)")
                .coalesce(1)
                .writeStream()
                .outputMode("append")
                .format("parquet")
                .option("compression", "snappy")
                .option("header", "true")
                .option("path","hdfs://10.140.0.13:9000/user/haiht34/data_tracking")
                .option("checkpointLocation","hdfs://10.140.0.13:9000/user/haiht34/checkpoint")
                .partitionBy("year","month","day","hour")
                .start();
        query.awaitTermination();
    }
}