package com.tuyoo.mysqltest;

import java.sql.Connection;
import java.sql.SQLException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.tuyoo.util.MysqlUtil;

public class SqlTest {
	
	Connection connection = null;

	@Test
	public void ConnectTest(){
		System.out.println(connection);
	}
	
	@Before
	public void setConn(){
		connection = MysqlUtil.getNewConnection();
	}
	
	@After
	public void closeConn(){
		if(connection != null){
			try {
				connection.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
}
