package com.flink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.types.Row;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.codehaus.jettison.json.JSONObject;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

public class HttpGetData extends RichSinkFunction<Row> {

	private Connection connection;
	private PreparedStatement preparedStatement;
	private Producer<String, String> producer;


	@Override
	public void open(Configuration parameters) throws Exception {
		//初始化kafka连接环境
		super.open(parameters);
		Properties props = new Properties();
		props.put("bootstrap.servers", "127.0.0.1:9092");
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		producer = new KafkaProducer<String, String>(props);


		//初始化mysql数据源连接环境
		String className = "com.mysql.jdbc.Driver";
		Class.forName(className);
		String url = "jdbc:mysql://172.19.20.76:3306/flink";
		//String url = "jdbc:mysql://192.168.120.160:3306/flink";
		String user = "root";
		String password = "root";
		connection = DriverManager.getConnection(url, user, password);
		//String sql = "select sum(money) as money from new_table_1 limit ?";
		String sql = "select  sum(a.money) as money from (select  orderno,money from new_table limit ?) a";
		//String sql = "select  sum(a.money) as money from (select  order_no,money from new_table_1 limit ?, ?) a group by order_no";
		preparedStatement = connection.prepareStatement(sql);
		super.open(parameters);
	}

	@Override
	public void close() throws Exception {

		if (preparedStatement != null) {
			preparedStatement.close();
		}
		if (connection != null) {
			connection.close();
		}
		super.close();
	}

	@Override
	public void invoke(Row value, Context context) throws Exception {

		Long ff1 = System.currentTimeMillis();
		try {
			//把kafka的数据作为参数去处理其他数据源
			String[] split = value.toString().split(",");
		/*	System.out.println(value.toString() + "999999999999999999999999999");
		    //mysql中查询sql的第一个条件
			preparedStatement.setInt(1, new Random().nextInt(100));
			//mysql中查询sql的第二个条件
			preparedStatement.setInt(2, Integer.parseInt(split[1]));
			//执行mysql查询操作
			ResultSet resultSet = preparedStatement.executeQuery();
			Float money = null;
			//处理查询结果
			while (resultSet.next()) {
				money = resultSet.getFloat("money");
			}
			System.out.println(money.toString() + "------------------------");
			Long ff2 = System.currentTimeMillis();
			System.out.printf("聚合计算时间=============：" + ((ff2 - ff1)) + "毫秒");
			JSONObject event = new JSONObject();
			event.put("f0",split[0]);
			event.put("f1",money);
			event.put("f2",ff2 - ff1);
			//把处理的结果放回KAFKA
			producer.send(new ProducerRecord<String, String>("topic.flink.mysql.response1",split[0], event.toString()));
*/

            //把某个接口信息作为查询的数据源
			String loginUrl =
				"http://172.19.20.76:8091/kafkaMsg/mysql/getData?recordCount=" + Integer.parseInt(
					split[1]);
			//获取接口信息
			String result1 = HttpUtil.get(loginUrl);
			//处理接口返回的结果
			List<Info> userInfos = JsonUtil.fromJsonArray(result1, Info.class);
			//对返回的结果做分类并做聚合
//			Map<String, Double> collect = userInfos.stream()
//				.collect(Collectors.groupingBy(
//					Info::getOrderNo,
//					Collectors.summingDouble(Info::getMoney)));
//			Long ff2 = System.currentTimeMillis();
//			JSONObject event = new JSONObject();
//			event.put("f0", split[0]);
//			event.put("f1", collect.toString());
//			event.put("f2", ff2 - ff1);
//			System.out.println(split[1] + "=========" + (ff2 - ff1));
//			//把处理的结果放回到消息队列
//			producer.send(new ProducerRecord<String, String>(
//				"topic.flink.mysql.response",
//				split[0],
//				event.toString()));

		} catch (Exception e) {
			e.printStackTrace();
		}
		//System.out.println("DeviceMap>>>>>>" + DeviceMap);

		// DeviceMap.clear();


	}
}
