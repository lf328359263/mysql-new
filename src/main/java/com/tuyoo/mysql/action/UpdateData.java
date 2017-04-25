package com.tuyoo.mysql.action;

import java.sql.Connection;
import java.sql.SQLException;

import com.tuyoo.util.MysqlUtil;

public class UpdateData {
	
	private Connection conn;
	
	public UpdateData(Connection conn){
		super();
		this.conn = conn;
	}
	
	public UpdateData(){
		this(MysqlUtil.getLocalConnection());
	}
	
	public String updateDayCount1(String day, String platform_id, String type, String value){
		String status = "fail";
		String sql = "update day_count_1 set value = JSON_SET(value, '$.\""+type+"\"', '"+ value +"') where day = '"+day+"' and platform_id = '"+platform_id+"'";
		try {
			System.out.println(sql);
			int execute = conn.createStatement().executeUpdate(sql);
			System.out.println(execute);
			status = "success";
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return status;
	}
	
	public String updateDayCount2(String day, String platform_id, String product_id, String type, String value){
		String status = "fail";
		String sql = "update day_count_2 set value = JSON_SET(value, '$.\""+type+"\"', '"+ value +"') where day = '"+day+"' and platform_id = '"+platform_id+"' and product_id = '"+product_id+"'";
		try {
			System.out.println(sql);
			int execute = conn.createStatement().executeUpdate(sql);
			System.out.println(execute);
			status = "success";
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return status;
	}
	
	public String updateDayCount3(String day, String platform_id, String product_id, String channel_id, String type, String value){
		String status = "fail";
		String sql = "update day_count_3 set value = JSON_SET(value, '$.\""+type+"\"', '"+ value +"') where day = '"+day+"' and platform_id = '"+platform_id+"' and product_id = '"+product_id+"' and channel_id = '"+channel_id+"'";
		try {
			System.out.println(sql);
			int execute = conn.createStatement().executeUpdate(sql);
			System.out.println(execute);
			status = "success";
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return status;
	}
	
	public String updateDayCount4(String day, String platform_id, String product_id, String channel_id, String product_nickname, String type, String value){
		String status = "fail";
		String sql = "update day_count_4 set value = JSON_SET(value, '$.\""+type+"\"', '"+ value +"') where day = '"+day+"' and platform_id = '"+platform_id+"' and product_id = '"+product_id+"' and product_nickname = '"+product_nickname+"' and channel_id = '"+channel_id+"'";
		try {
			System.out.println(sql);
			int execute = conn.createStatement().executeUpdate(sql);
			System.out.println(execute);
			status = "success";
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return status;
	}
	
	public void close(){
		if(conn != null){
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
}
