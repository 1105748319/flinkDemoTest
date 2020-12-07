package com.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.Properties;

public class TestMain {
	protected static StreamExecutionEnvironment env;
	protected static StreamTableEnvironment tEnv;
	protected static int ff = 0;

	public static void main(String[] args) throws Exception {
		try {
			env = StreamExecutionEnvironment.getExecutionEnvironment();

			tEnv = StreamTableEnvironment.create(
				env,
				EnvironmentSettings.newInstance()
					// Watermark is only supported in blink planner
					.useBlinkPlanner()
					.inStreamingMode()
					.build()
			);
			env.getConfig().setRestartStrategy(RestartStrategies.noRestart());
			env.enableCheckpointing(1000);


			String createTable = String.format(
				//"CREATE TABLE UserScores (type STRING, orderNo STRING,productName STRING,money FLOAT,name STRING,zoneCode STRING,zoneName STRING)\n" +
				"CREATE TABLE UserScores (requestId STRING, dataList ARRAY<ROW(orderNo STRING, money FLOAT, name STRING, zoneCode STRING, zoneName STRING)>)\n"
					+
					"WITH (\n" +
					"  'connector' = 'kafka',\n" +
					"  'topic' = 'topic.flink',\n" +
					"  'properties.bootstrap.servers' = '127.0.0.1:9092',\n" +
					"  'properties.group.id' = 'testGroup1',\n" +
					"  'format' = 'json',\n" +
					"  'scan.startup.mode' = 'latest-offset'\n" +
					")");


			TableResult tableResult = tEnv.executeSql(createTable);

			Table table = tEnv.sqlQuery("SELECT * FROM UserScores ");
			tEnv.registerTable("result_agg", table);
			table.printSchema();
		/*Table query = tEnv.sqlQuery("select sum(dataList) from result_agg");
		//DataSet<Row> ds = tEnv.toDataSet(query, Row.class);
		DataStream<Tuple2<Boolean, Row>> ds = tEnv.toRetractStream(query,Row.class);
		ds.print();*/

			DataStream<Row> infoDataStream1 = tEnv.toAppendStream(table, Row.class);
			SingleOutputStreamOperator<Tuple2<String, Float>> stringSingleOutputStreamOperator = infoDataStream1
				.flatMap(
					new FlatMapFunction<Row, Tuple2<String, Float>>() {
						@Override
						public void flatMap(Row value, Collector<Tuple2<String, Float>> out)
							throws Exception {
							String[] s = value.toString().split("\\[")[1].split("]")[0].split(" ");
							float pp = 0;
							for (String s1 : s) {
								//Double aDouble = new Double(s1.split(",")[1].toString());
								pp += Float.valueOf(s1.split(",")[1]);
							}
							System.out.println( "=======");
							//System.out.println(ff.split(" ")[0].split(",")[1]);
							out.collect(Tuple2.of(
								value.toString().split(",")[0], pp));
							//out.collect( "666=======");
							//value.toString();

						}
					});
			System.out.println("fsdf");
			Properties properties = new Properties();
			properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
			properties.setProperty("transaction.timeout.ms", 1000 * 60 * 5 + "");
			//properties.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 1000 * 60 * 3 + "");
			//properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1");
			properties.setProperty("max.in.flight.requests.per.connection", "1");
			//properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
			properties.setProperty("enable.idempotence", "true");
			String topic = "topic.flink.response";
			FlinkKafkaProducer<Tuple2<String, Float>> producer = new FlinkKafkaProducer<>(
				topic,
				new ObjSerializationSchema(topic),
				properties,
				FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
			stringSingleOutputStreamOperator.print();
			stringSingleOutputStreamOperator.keyBy(0).sum(1).addSink(producer);


		/*Long ff = System.currentTimeMillis();

		Long ff1 = System.currentTimeMillis();
		System.out.printf("getProject计算时间：" + ((ff1 - ff)) + "毫秒");
		DataStreamSource<UserInfo> infoDataStreamSource = env.addSource(new MySQLWriter());
		//infoDataStreamSource.print();
		tEnv.registerDataStream("prcun", infoDataStreamSource);
		if (ff == 1) {
			Table query = tEnv.sqlQuery("select * from prcun ");
			DataStream<Row> rowDataStream = tEnv.toAppendStream(query, Row.class);
			//DataStream<Tuple2<Boolean, Row>> tuple2DataStream = tEnv.toRetractStream(query, Row.class);
			rowDataStream.print();
		}

		Long ff2 = System.currentTimeMillis();
		System.out.printf("聚合计算时间：" + ((ff2 - ff1)) + "毫秒");*/


			env.execute();

		} catch (Exception e) {

		}


	}
}
