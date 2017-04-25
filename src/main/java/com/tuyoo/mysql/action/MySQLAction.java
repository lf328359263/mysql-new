package com.tuyoo.mysql.action;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.log4j.Logger;

import com.tuyoo.tmp.FilePersist;
import com.tuyoo.util.MysqlUtil;

public class MySQLAction {
	
	Connection connection = null;
	private static Logger logger = Logger.getLogger(MySQLAction.class);
	
//	public void executeQuery(){
//		String sql = "";
//		try {
//			PreparedStatement prepareStatement = connection.prepareStatement(sql);
//			prepareStatement.s
//		} catch (SQLException e) {
//			e.printStackTrace();
//		}
//	}
	
	public void executeQueryAndShow(String sql){
		ResultSet resultSet = executeQuery(sql);
		if(resultSet != null){
			try {
				ResultSetMetaData metaData = resultSet.getMetaData();
				int count = metaData.getColumnCount();
				while(resultSet.next()){
					StringBuffer sb = new StringBuffer();
					for(int i = 0; i < count; i++){
						sb.append(resultSet.getString(i+1)).append("\t");
					}
					System.out.println(sb.toString());
				}
			} catch (SQLException e) {
				logger.error(e.getMessage());
			}
		}
	}
	
	public ResultSet executeQuery(String sql){
		try {
			logger.info("执行查询： " + sql);
			return connection.createStatement().executeQuery(sql);
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	public void persistToFile(ResultSet resultSet, String path, String name){
		FilePersist filePersist = new FilePersist(path, name);
		try {
			int len = resultSet.getMetaData().getColumnCount();
			while(resultSet.next()){
				StringBuffer sb = new StringBuffer();
				for(int i = 0; i<len; i++){
					if(i!= 0){
						sb.append("\t");
					}
					sb.append(resultSet.getString(i+1));
				}
				filePersist.writeLine(sb.toString());
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		filePersist.close();
	}
	
	public void execute(String sql){
		try {
			logger.info("执行查询： " + sql);
			Statement statement = connection.createStatement();
			statement.execute(sql);
			logger.info("执行完毕： " + sql);
			statement.close();
		} catch (SQLException e) {
			logger.error(e.getMessage());
			e.printStackTrace();
		}
	}
	
	public MySQLAction(){
		this(MysqlUtil.getNewConnection());
	}
	
	public MySQLAction(Connection conn){
		super();
		connection = conn;
	}
	
	public void close(){
		if(connection != null){
			try {
				logger.info("关闭连接。。。");
				connection.close();
				logger.info("连接已关闭。。。");
			} catch (SQLException e) {
				logger.error(e.getMessage());
			}
		}
	}
}
