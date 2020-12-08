package com.flink;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

public class TestMain {
    protected static StreamExecutionEnvironment env;
    protected static StreamTableEnvironment tEnv;
    protected static int ff = 0;

    public static void main(String[] args) throws Exception {
        env = StreamExecutionEnvironment.getExecutionEnvironment();

        tEnv = StreamTableEnvironment.create(env, EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build()
        );
        env.getConfig().setRestartStrategy(RestartStrategies.noRestart());
        env.enableCheckpointing(1000);
        String createTable = String.format(
                //"CREATE TABLE UserScores (type STRING, orderNo STRING,productName STRING,money FLOAT,name STRING,zoneCode STRING,zoneName STRING)\n" +
                "CREATE TABLE UserScores (requestId STRING, recordCount  STRING)\n"
                        +
                        "WITH (\n" +
                        "  'connector' = 'kafka',\n" +
                        "  'topic' = 'topic.flink.mysql3',\n" +
                        "  'properties.bootstrap.servers' = '192.168.120.130:9092',\n" +
                        "  'properties.group.id' = 'testGroup1',\n" +
                        "  'format' = 'json',\n" +
                        "  'scan.startup.mode' = 'group-offsets'\n" +
                        ")");

        TableResult tableResult = tEnv.executeSql(createTable);
        Table table = tEnv.sqlQuery("SELECT * FROM UserScores ");
        DataStream<UserInfo> infoDataStream1 = tEnv.toAppendStream(table, UserInfo.class);
        // infoDataStream1.print();

        String createTable1 = String.format(
                //"CREATE TABLE UserScores (type STRING, orderNo STRING,productName STRING,money FLOAT,name STRING,zoneCode STRING,zoneName STRING)\n" +
                "CREATE TABLE new_table_copy1 (type STRING, order_no STRING,product_name STRING,money FLOAT,name STRING,zone_Code STRING,zone_name STRING)\n"
                        +
                        "WITH (\n" +
                        "  'connector' = 'jdbc',\n" +
                        "  'url' = 'jdbc:mysql://192.168.120.160:3306/flink?useSSL=false',\n" +
                        "  'table-name' = 'new_table_copy1',\n" +
                        "  'username' = 'root',\n" +
                        "  'password' = 'root'\n" +
                        ")");
        TableResult tableResult1 = tEnv.executeSql(createTable1);
        Table table1 = tEnv.sqlQuery("SELECT * FROM new_table_copy1 ");
        DataStream<Info> infoDataStream = tEnv.toAppendStream(table1, Info.class);
        //infoDataStream.print();
        DataStream<String> apply = infoDataStream.join(infoDataStream1).where(info -> info.getOrder_no()).equalTo(in -> in.getRequestId())
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .apply(new JoinFunction<Info, UserInfo, String>() {
                    @Override
                    public String join(Info info, UserInfo userInfo) throws Exception {
                        return info.getOrder_no() + "==========" + userInfo.getRequestId();
                    }
                });
        apply.print();
            /*Properties properties = new Properties();
            properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
            properties.setProperty("transaction.timeout.ms", 1000 * 60 * 5 + "");
            properties.setProperty("max.in.flight.requests.per.connection", "1");
            properties.setProperty("enable.idempotence", "true");
            String topic = "topic.flink.response";
            FlinkKafkaProducer<Tuple2<String, Float>> producer = new FlinkKafkaProducer<>(
                    topic,
                    new ObjSerializationSchema(topic),
                    properties,
                    FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
            stringSingleOutputStreamOperator.print();
            stringSingleOutputStreamOperator.keyBy(0).sum(1).addSink(producer);*/
        env.execute();
    }

    /**
     * Right Join
     * 获取每个用户每个时刻的点击。有浏览则顺带输出，没有则浏览置空。
     */
    static class RightJoinFunction implements CoGroupFunction<Row, Row, String> {
        @Override
        public void coGroup(Iterable<Row> left, Iterable<Row> right, Collector<String> out) throws Exception {

            for (Row userClickLog : right) {
                boolean noElements = true;
                for (Row userBrowseLog : left) {
                    noElements = false;
                    out.collect(userBrowseLog + " ==Right Join=> " + userClickLog);
                }

                if (noElements) {
                    out.collect("null" + " ==Right Join=> " + userClickLog);
                }
            }
        }
    }
}
