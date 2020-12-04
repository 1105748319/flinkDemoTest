package com.flink;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class TestMainMysql {
	protected static StreamExecutionEnvironment env;
	protected static StreamTableEnvironment tEnv;

	public static void main(String[] args) throws Exception {
		//初始化flinkStream环境
		env = StreamExecutionEnvironment.getExecutionEnvironment();
		//初始化flinkStreamTable环境
		tEnv = StreamTableEnvironment.create(
			env,
			EnvironmentSettings.newInstance()
				.useBlinkPlanner()
				.inStreamingMode()
				.build()
		);
		//数据源涉及到kafka所以我们这里设置检查点，每秒去检查一次避免kafka消息没有消费
		env.enableCheckpointing(1000);
		//读取kafka的数据并创建一张叫UserScores的表字段为requestId，recordCount
		String createTable = String.format(
			"CREATE TABLE UserScores (requestId STRING,recordCount STRING)\n" +
				//"CREATE TABLE UserScores (requestId STRING, dataList ARRAY<ROW(orderNo STRING, money FLOAT, name STRING, zoneCode STRING, zoneName STRING)>)\n"+
				"WITH (\n" +
				"  'connector' = 'kafka',\n" +
				"  'topic' = 'topic.flink.mysql',\n" +
				"  'properties.bootstrap.servers' = '127.0.0.1:9092',\n" +
				"  'properties.group.id' = 'testGroup1',\n" +
				"  'format' = 'json',\n" +
				"  'scan.startup.mode' = 'group-offsets'\n" +
				")");
		TableResult tableResult = tEnv.executeSql(createTable);
		//设置表UserScores的执行sql
		Table table = tEnv.sqlQuery("SELECT * FROM UserScores ");
		//通过sql的形式读取出kafka的数据
		DataStream<Row> infoDataStream1 = tEnv.toAppendStream(table, Row.class);
		//处理kafka的数据可以作为其他数据源的参数使用
		infoDataStream1.addSink(new HttpGetData());
		env.execute();


	}
}
