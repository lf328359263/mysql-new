package com.tuyoo.mysql.action;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.json.JSONObject;

import com.tuyoo.util.MysqlUtil;

public class DataTrans {
	
	private Map<String, JSONObject> allMap = new HashMap<String, JSONObject>();
	
	private JSONObject getJson(String key){
		if(!allMap.containsKey(key)){
			allMap.put(key, new JSONObject());
		}
		return allMap.get(key);
	}
	
	public void writeToNewMysql(String replace){
		System.out.println(new Date() + " 执行批量写入...");
		Connection conn = MysqlUtil.getNewConnection();
		try {
			conn.setAutoCommit(false);
			PreparedStatement statement = conn.prepareStatement(replace);
			for (String[] ss : transToList()) {
				for(int i = 0; i<ss.length; i++){
					statement.setString(i+1, ss[i]);
				}
				statement.addBatch();
			}
			statement.executeBatch();
			conn.commit();
			statement.clearBatch();
			statement.close();
			conn.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		System.out.println(new Date() + " 执行完毕...");
	}
	
	private ArrayList<String[]> transToList(){
		ArrayList<String[]> lines = new ArrayList<String[]>();
		for (Entry<String, JSONObject> entry : allMap.entrySet()) {
			String[] split = (entry.getKey()+"_"+entry.getValue().toString()).split("_");
			lines.add(split);
		}
		return lines;
	}
	
	public void cache(String sql, Map<String, String> map){
		cache(MysqlUtil.getOldConnection(), sql, map);
	}
	
	public void cache(Connection conn, String sql, Map<String, String> map){
		MySQLAction mySQLAction = new MySQLAction(conn);
		ResultSet resultSet = mySQLAction.executeQuery(sql);
		try {
			ResultSetMetaData metaData = resultSet.getMetaData();
			int columnCount = metaData.getColumnCount();
			while(resultSet.next()){
				StringBuffer sb = new StringBuffer();
				for(int i = 0; i<columnCount-2; i++){
					if(i!=0){
						sb.append("_");
					}
					sb.append(resultSet.getString(i+1));
				}
				JSONObject json = getJson(sb.toString());
				String k = resultSet.getString(columnCount-1);
				String v = resultSet.getString(columnCount);
				if(map.containsKey(k)){
					k = map.get(k);
				}
				if(json.has(k)){
					System.out.println(json.get(k)+" ======> "+v);
				}
				json.put(k, v);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		mySQLAction.close();
	}
	
	public void persistResultSetToMysql(ResultSet resultSet, String sql, int columns){
		System.out.println(new Date() + " 执行批量写入...");
		Connection conn = null;
		try {
			conn = MysqlUtil.getNewConnection();
			conn.setAutoCommit(false);
			PreparedStatement statement = conn.prepareStatement(sql);
			while(resultSet.next()){
				for(int i = 0; i<columns; i++){
					statement.setString(i+1, resultSet.getString(i+1));
				}
				statement.addBatch();
			}
			statement.executeBatch();
			conn.commit();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if(conn!= null){
				try {
					conn.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
		System.out.println(new Date() + " 写入完毕...");
	}
	
	public void persistResultSetToMysql(ResultSet resultSet, String tableName){
		try {
			ResultSetMetaData metaData = resultSet.getMetaData();
			int columns = metaData.getColumnCount();
			StringBuffer sb = new StringBuffer("replace into ");
			sb.append(tableName);
			sb.append(" values (");
			for(int i = 0; i<columns; i++){
				if(i != 0){
					sb.append(",");
				}
				sb.append("?");
			}
			sb.append(")");
			persistResultSetToMysql(resultSet, sb.toString(), columns);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
}
