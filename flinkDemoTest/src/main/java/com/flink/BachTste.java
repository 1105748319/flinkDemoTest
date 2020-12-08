package com.flink;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BachTste {

	public static void main(String[] args) throws Exception {
		System.out.printf("开始+++++++++++++++++++++++++++++++++++");
		 /* String driverClass = "com.mysql.jdbc.Driver";
        String dbUrl = "jdbc:mysql://192.168.120.160:3306/prestodb?serverTimezone=GMT%2B8&useSSL=false";
        String userNmae = "root";
        String passWord = "root";*/
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		// BatchTableEnvironment tableEnv = new BatchTableEnvironment();
		//tableEnv.registerFunction("mapToString", new MapToString());
		BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);
		Long ff = System.currentTimeMillis();
		getProject(env, tEnv);
		Long sta1 = System.currentTimeMillis();
		System.out.printf("getProject计算时间：" + ((sta1 - ff)) + "毫秒");
		;
		//getUser(env, tEnv,args);
		Long sta2 = System.currentTimeMillis();
		System.out.printf("getUser计算时间：" + ((sta2 - sta1)) + "毫秒");
		;
		//joinTableProjectWithInfo(tEnv);
		Long sta3 = System.currentTimeMillis();
		System.out.printf("joinTableProjectWithInfo计算时间：" + ((sta3 - sta2)) + "毫秒");
		;

		//Table query = tEnv.sqlQuery("select usercode,username from userfff");
		Table query = tEnv.sqlQuery("select count(1),orderno from project group by orderno ");
		DataSet<Row> ds = tEnv.toDataSet(query, Row.class);
		ds.print();
		Long sta4 = System.currentTimeMillis();
		System.out.printf("dddddddd============计算时间：" + ((sta4 - sta3)) + "毫秒");
		;
		/*ds.map(new MapFunction<TestInfo, Tuple2<String, String>>() {
			@Override
			public Tuple2<String, String> map(Row row) throws Exception {
				java.text.DecimalFormat df = new java.text.DecimalFormat("###############0.00");
				String format = df.format(row.getMoney());
				Properties props = new Properties();
				props.put("bootstrap.servers", "192.168.120.130:9092");
				props.put("acks", "all");
				props.put("retries", 0);
				props.put("batch.size", 16384);
				props.put("linger.ms", 1);
				props.put("buffer.memory", 33554432);
				props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
				props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
				Producer<String, String> producer = new KafkaProducer<String, String>(props);
				producer.send(new ProducerRecord<String, String>("nima",row.getMoney().toString(), format+","+row.getUsername()));
				producer.close();
				return new Tuple2<String, String>(format, row.getUsername());
			}
		}).count();*/
		Long sta5 = System.currentTimeMillis();
		System.out.printf("count============计算时间：" + ((sta5 - sta4)) + "毫秒");
		;
	}

	public static void getProject(ExecutionEnvironment env, BatchTableEnvironment tableEnv) {

		TypeInformation[] fieldTypes = new TypeInformation[]{BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.FLOAT_TYPE_INFO};
		String[] fieldNames = new String[]{"orderno", "money"};
		RowTypeInfo rowTypeInfo = new RowTypeInfo(fieldTypes, fieldNames);
		JDBCInputFormat jdbcInputFormat = JDBCInputFormat
			.buildJDBCInputFormat()
			.setDrivername("com.mysql.jdbc.Driver")
			.setDBUrl("jdbc:mysql://192.168.120.160:3306/flink?serverTimezone=GMT%2B8&useSSL=false")
			.setUsername("root")
			.setPassword("root")
			.setQuery("select order_no as orderno, money from new_table_1 limit 10000")
			.setRowTypeInfo(rowTypeInfo)
			.finish();
		DataSource<Row> s = env.createInput(jdbcInputFormat);
		tableEnv.registerDataSet("project", s);
	}

	public static void getUser(ExecutionEnvironment env, BatchTableEnvironment tableEnv,String[] st) {

		try {
			Integer integer = Integer.parseInt(st[0]);
			System.out.println("=============现在数据量"+integer);
			List<Info> objects = new ArrayList<>();
			for (int i = 0; i < integer; i++) {
				Info info = new Info();
				info.setMoney((float) i);
				//info.setOrderNo("ffff");
				objects.add(info);
			}
			DataSource<Info> userInfoDataSource = env.fromCollection(objects);
			tableEnv.registerDataSet("userfff", userInfoDataSource);
			/*Map<String, String> header = new HashMap<>(16);
			header.put("Content-Type", "application/json; charset=utf-8");
			String loginUrl = "http://127.0.0.1:9090/book/test2";
			String s = HttpUtil.get(loginUrl);
			List<Info> userInfos = JsonUtil.fromJsonArray(s, UserInfo.class);
			DataSource<UserInfo> userInfoDataSource = env.fromCollection(userInfos);
			tableEnv.registerDataSet("userfff", userInfoDataSource);*/
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
//
//	public static void joinTableProjectWithInfo(BatchTableEnvironment tableEnv) {
//		Table result = tableEnv.sqlQuery("select a.orderno , money , b.username  from project a INNER JOIN  userfff b on a.orderno=b.usercode limit 0,z`11000");
//		tableEnv.registerTable("result_agg", result);
//		result.printSchema();
//	}
}
