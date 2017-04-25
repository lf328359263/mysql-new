package com.tuyoo.mysqltest;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.tuyoo.mysql.action.MySQLAction;

public class ClassMethodTest {
	
	MySQLAction mySQLAction = null;
	
	
//	@Test
	public void exeTest(){
		String sql = "CREATE TABLE user (id INT PRIMARY KEY, name VARCHAR(20) , lastlogininfo JSON)";
		mySQLAction.execute(sql);
	}
	
//	@Test
	public void insertTest(){
//		UPDATE `user` SET lastlogininfo = JSON_SET(lastlogininfo ,"$.netmask","192.168.1.1") where `name` = 'xxx';
		String sql = "INSERT INTO user VALUES(4 ,\"monkey\",'{\"time\":\"2015-01-01 13:00:00\",\"ip\":\"192.168.1.1\",\"result\":\"fail\"}')";
		mySQLAction.execute(sql);
	}
	
	@Test
	public void selectTest(){
		String sql = "select * from day_count_1 where day = '2017-01-01' and platform_id = '11'";
		ResultSet resultSet = mySQLAction.executeQuery(sql);
		try {
			if(resultSet.next()){
				System.out.println("有记录。。。");
			} else {
				System.out.println("没有记录。。。");
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} 
	}
	
	@After
	public void close(){
		mySQLAction.close();
	}
	
	@Before
	public void init(){
		mySQLAction=new MySQLAction();
	}
}
