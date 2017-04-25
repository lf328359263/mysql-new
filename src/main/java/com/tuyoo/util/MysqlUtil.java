package com.tuyoo.util;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.log4j.Logger;

public class MysqlUtil {

	private static Logger logger = Logger.getLogger(MysqlUtil.class);

	public static Connection getMySQLConnection() {
		Connection conn = null;
		Properties properties = new Properties();
		try {
			FileInputStream fileInputStream = new FileInputStream("/Users/Administrator.tuyou-004-PC/workspace/tuyoo-scala/mysql-new/src/main/java/db.properties");
			properties.load(fileInputStream);
			logger.info("读取配置文件。。。");
		} catch (IOException e1) {
			logger.error(e1.getMessage());
		}
		String driverClass = properties.getProperty("3308.driverClass", "com.mysql.cj.jdbc.Driver");
		String user = properties.getProperty("3308.user", "root");
		String password = properties.getProperty("3308.password", "tuyougame");
		String url = properties.getProperty("3308.url", "jdbc:mysql://10.3.0.50:3308/datacenter?zeroDateTimeBehavior=convertToNull&useUnicode=true&characterEncoding=UTF-8&useSSL=false&autoReconnect=true");
		
		try {
			Class.forName(driverClass);
			conn = DriverManager.getConnection(url, user, password);
			logger.info("获取mysql连接。。。");
		} catch (ClassNotFoundException | SQLException e) {
			logger.error(e.getMessage());
		}
		return conn;
	}
	
	public static Connection getNewConnection(){
		try {
			Class.forName("com.mysql.jdbc.Driver");
			return DriverManager.getConnection("jdbc:mysql://10.3.0.50:3308/datacenter?characterEncoding=UTF-8&useSSL=false", "tuyoo", "tuyougame");
		} catch (ClassNotFoundException | SQLException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	public static Connection getOldConnection(){
		try {
			Class.forName("com.mysql.jdbc.Driver");
			return DriverManager.getConnection("jdbc:mysql://10.3.0.50:3307/result_test", "tuyoo", "tuyougame");
		} catch (ClassNotFoundException | SQLException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	public static Connection getLocalConnection(){
		try {
			Class.forName("com.mysql.jdbc.Driver");
			return DriverManager.getConnection("jdbc:mysql:///datacenter?characterEncoding=UTF-8&useSSL=false", "root", "123456");
		} catch (ClassNotFoundException | SQLException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	public static Connection getConnection_51(){
		try {
			Class.forName("com.mysql.jdbc.Driver");
			return DriverManager.getConnection("jdbc:mysql://10.3.0.51:3306/datacenter_db_new", "tuyou", "tuyoogame");
		} catch (ClassNotFoundException | SQLException e) {
			e.printStackTrace();
			return null;
		}
	}
	
}
