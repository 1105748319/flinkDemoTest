package com.flink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

public class MySQLWriter extends RichSourceFunction<UserInfo> {

	private Connection connection = null;
	private PreparedStatement ps = null;
	private volatile boolean isRunning = true;


	//该方法主要用于打开数据库连接，下面的ConfigKeys类是获取配置的类
	@Override
	public void open(Configuration parameters) throws Exception {


		String className = "com.mysql.jdbc.Driver";
		Class.forName(className);
		String url = "jdbc:mysql://192.168.120.160:3306/flink";
		String user = "root";
		String password = "root";
		connection = DriverManager.getConnection(url, user, password);
		String sql = "select order_no as orderno,money from new_table_copy1 limit 100";
		ps = connection.prepareStatement(sql);
		super.open(parameters);
	}


	//执行查询并获取结果
	@Override
	public void run(SourceContext<UserInfo> ctx) throws Exception {

		//while (isRunning) {
		/*	Long ff1 = System.currentTimeMillis();
			Map<String, String> header = new HashMap<>(16);
			header.put("Content-Type", "application/json; charset=utf-8");
			String loginUrl = "http://127.0.0.1:9090/book/test";
			String s = HttpUtil.get(loginUrl);
			List<UserInfo> userInfos = JsonUtil.fromJsonArray(s, UserInfo.class);
			for (UserInfo user: userInfos) {
				//System.out.printf("+++++++++++"+user.getUsername()+"	"+ user.getUsercode());
				ctx.collect(user);
			}*/

			ResultSet resultSet = ps.executeQuery();
			while (resultSet.next()) {
				UserInfo DeviceMap = new UserInfo();
				String orderno = resultSet.getString("orderno");
				Float money = resultSet.getFloat("money");
				if (!(orderno.isEmpty())) {
					DeviceMap.setOrderno(orderno);
					DeviceMap.setMoney(money);
					ctx.collect(DeviceMap);
				}
			}

			//System.out.println("DeviceMap>>>>>>" + DeviceMap);

			//DeviceMap.clear();
			Long ff2 = System.currentTimeMillis();
			//System.out.printf("聚合计算时间=============：" + ((ff2 - ff1)) + "毫秒");
			//Thread.sleep(1000 * 10);

		//}


	}


	@Override
	public void cancel() {
		try {
			super.close();
			if (connection != null) {
				connection.close();
			}
			if (ps != null) {
				ps.close();
			}
		} catch (Exception e) {
			System.out.printf("runException" + e);
		}
		isRunning = false;

	}


}
