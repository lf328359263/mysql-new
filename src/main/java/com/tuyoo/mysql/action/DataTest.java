package com.tuyoo.mysql.action;

import com.tuyoo.util.MysqlUtil;

public class DataTest {

	public static void main(String[] args) {
		String day = "2016-06-01";
		String sql = "select day, platform_id, product_id, '127' type, sum(value) value from regress_count where day = '"+day+"' and type in ('123','124') and platform_id > 0 and product_id > 0 and game_id = '-1' and channel_id = '-1' and product_nickname = '-1' group by day, platform_id, product_id";
		
		MySQLAction mySQLAction = new MySQLAction(MysqlUtil.getOldConnection());
//		MySQLAction mySQLAction = new MySQLAction(MysqlUtil.getConnection_51());
		mySQLAction.executeQueryAndShow(sql);
		mySQLAction.close();
	}
}
