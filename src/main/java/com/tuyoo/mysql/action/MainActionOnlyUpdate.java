package com.tuyoo.mysql.action;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;

import com.tuyoo.util.MysqlUtil;

public class MainActionOnlyUpdate {
	
	public static void main(String[] args) {
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		String minDay = "2017-04-10";
		String maxDay = "2017-04-10";
		String index = "1";
		if(args.length == 3){
			minDay = args[0];
			maxDay = args[1];
			index = args[2];
		}
		Calendar calendar_max = Calendar.getInstance();
		Calendar calendar_min = Calendar.getInstance();
		try {
			calendar_min.setTime(simpleDateFormat.parse(minDay));
			calendar_max.setTime(simpleDateFormat.parse(maxDay));
			while(calendar_max.compareTo(calendar_min) >= 0){
				String day = simpleDateFormat.format(calendar_max.getTime());
				System.out.println(day);
				List<String> list = Arrays.asList(index.replaceAll(" ", "").split(","));
				if(list.contains("1")){
					exe1(day);
				}
				if(list.contains("2")){
					exe2(day);
				}
				if(list.contains("3")){
					exe3(day);
				}
				if(list.contains("4")){
					exe4(day);
				}
				calendar_max.add(Calendar.DATE, -1);
			}
		} catch (ParseException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 平台-产品 day_count 更新新增和召回数据
	 * @param day
	 */
	public static void exe1(String day){
		String sql = "select day, platform_id, type, sum(value) value from regress_count where day = '"+day+"' and type in ('123','124') and platform_id > 0 and product_id > 0 and game_id = '-1' and channel_id > 0 and product_nickname > 0 group by day, platform_id, type";
		MySQLAction action = new MySQLAction(MysqlUtil.getOldConnection());
		ResultSet resultSet = action.executeQuery(sql);
		UpdateData data = new UpdateData(MysqlUtil.getNewConnection());
		try {
			while(resultSet.next()){
				data.updateDayCount1(resultSet.getString("day"), resultSet.getString("platform_id"), resultSet.getString("type"), resultSet.getString("value"));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		action.close();
		data.close();
	}
	
	/**
	 * 平台-产品 day_count 更新新增和召回数据
	 * @param day
	 */
	public static void exe2(String day){
		String sql = "select day, platform_id, product_id, type, sum(value) value from regress_count where day = '"+day+"' and type in ('123','124') and platform_id > 0 and product_id > 0 and game_id = '-1' and channel_id > 0 and product_nickname > 0 group by day, platform_id, product_id, type";
		MySQLAction action = new MySQLAction(MysqlUtil.getOldConnection());
		ResultSet resultSet = action.executeQuery(sql);
		UpdateData data = new UpdateData(MysqlUtil.getNewConnection());
		try {
			while(resultSet.next()){
				data.updateDayCount2(resultSet.getString("day"), resultSet.getString("platform_id"), resultSet.getString("product_id"), resultSet.getString("type"), resultSet.getString("value"));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		action.close();
		data.close();
	}
	
	/**
	 * 平台-产品-渠道 day_count 更新新增和召回数据
	 * @param day
	 */
	public static void exe3(String day){
		String sql = "select day, platform_id, product_id, channel_id, type, sum(value) value from regress_count where day = '"+day+"' and type in ('123','124') and platform_id > 0 and product_id > 0 and game_id = '-1' and channel_id > 0 and product_nickname > 0 group by day, platform_id, product_id, channel_id, type";
		MySQLAction action = new MySQLAction(MysqlUtil.getOldConnection());
		ResultSet resultSet = action.executeQuery(sql);
		UpdateData data = new UpdateData(MysqlUtil.getNewConnection());
		try {
			while(resultSet.next()){
				data.updateDayCount3(resultSet.getString("day"), resultSet.getString("platform_id"), resultSet.getString("product_id"), resultSet.getString("channel_id"), resultSet.getString("type"), resultSet.getString("value"));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		action.close();
		data.close();
	}
	
	/**
	 * 平台-产品-渠道 day_count 更新新增和召回数据
	 * @param day
	 */
	public static void exe4(String day){
		String sql = "select day, platform_id, product_id, channel_id, product_nickname, type, sum(value) value from regress_count where day = '"+day+"' and type in ('123','124') and platform_id > 0 and product_id > 0 and game_id = '-1' and channel_id > 0 and product_nickname > 0 group by day, platform_id, product_id, channel_id, product_nickname, type";
		MySQLAction action = new MySQLAction(MysqlUtil.getOldConnection());
		ResultSet resultSet = action.executeQuery(sql);
		UpdateData data = new UpdateData(MysqlUtil.getNewConnection());
		try {
			while(resultSet.next()){
				data.updateDayCount4(resultSet.getString("day"), resultSet.getString("platform_id"), resultSet.getString("product_id"), resultSet.getString("channel_id"), resultSet.getString("product_nickname"), resultSet.getString("type"), resultSet.getString("value"));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		action.close();
		data.close();
	}
	
}
