package com.tuyoo.tmp;

import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;

import com.tuyoo.mysql.action.MySQLAction;
import com.tuyoo.util.MysqlUtil;

public class DataToFile {
	
	Map<String, String> game = new HashMap<String, String>();
	
	public static void main(String[] args) {
		String sql = "select platform_id, product_id, value, type from base_test where day = '2016-12-21' and game_id = -1 and channel_id = -1 and product_id > 0 and platform_id > 0";
		MySQLAction mySQLAction = new MySQLAction(MysqlUtil.getOldConnection());
		ResultSet resultSet = mySQLAction.executeQuery(sql);
		mySQLAction.persistToFile(resultSet, "/home/gdss", "tmp.txt");
		mySQLAction.close();
	}
}
