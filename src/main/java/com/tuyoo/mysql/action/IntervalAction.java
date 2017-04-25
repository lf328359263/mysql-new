package com.tuyoo.mysql.action;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import com.tuyoo.util.DateUtil;

public class IntervalAction {
	
	public static Integer[] ltvIntervals = {0,1,6,14,29,59,89,119};
//	public static Integer[] ltvIntervals = {0,1};
	public static int ltvWeeks = 12;
	
//	LTV日汇总数据
	public static String getCycleDataSql(String level, String day){
		String reg_date = DateUtil.getDates(day, ltvIntervals);
		return "select reg_date, " + level +", DATEDIFF(day, reg_date) intervals, type, sum(value) from ltv_count where reg_date in ("+ reg_date+") and day = '"+ day +"' and type = 65 and product_id > 0 and channel_id > 0 and product_nickname > 0 and platform_id > 0 group by reg_date, "+level+", intervals, type";
	}
	
//	LTV日充值金额和人数
	public static String getCycleDataSql4(String level, String day){
		String reg_date = DateUtil.getDates(day, ltvIntervals);
		return "select reg_date, " + level +", DATEDIFF(day, reg_date) intervals, type, sum(value) from ltv_count where reg_date in ("+ reg_date+") and day = '"+ day +"' and type in (65,66) and product_id > 0 and channel_id > 0 and product_nickname > 0 and platform_id > 0 group by reg_date, "+level+", intervals, type";
	}
	
//	LTV周汇总数据
	public static String getLtvWeeksSql(String level, String day){
		String reg_date = DateUtil.getPreWeeksDates(day, ltvWeeks);
		return "select reg_date, " + level +", DATEDIFF(date_sub(day,interval -1 day), reg_date)/7 intervals, sum(value) from ltv_count where reg_date in ("+ reg_date+") and day = '"+ day +"' and type = 65 and product_id > 0 and channel_id > 0 and product_nickname > 0 and platform_id > 0 group by reg_date, "+level+", intervals";
	}
	
//	LTV月汇总数据
	public static List<String> getMonthDataSql(String level, String day){
		List<String> result = new ArrayList<String>();
		Calendar calendar = Calendar.getInstance();
		try {
			calendar.setTime(DateUtil.simpleDateFormat.parse(day));
			int i = calendar.get(Calendar.DAY_OF_MONTH);
			if(i == 1){
				String yesterday = DateUtil.getYesterday(day);
				int m = calendar.get(Calendar.MONTH);
				if(m==0){
					m=12;
				}
				for(i=m-1; i >= 0; i--){
//				for(i=m-1; i >= 5; i--){
					String[] ms = DateUtil.getPreviousMonthDate(day, m-i);
					if(ms.length > 1){
						String reg_date_1 = ms[0];
						String reg_date_2 = ms[1];
						String sql = "select '" + reg_date_1 + "', " + level +", '"+ m + "', sum(value) from ltv_count where reg_date between '"+reg_date_1+"' and '"+reg_date_2+"' and day = '" + yesterday +"' and type = 65 and product_id > 0 and channel_id > 0 and product_nickname > 0 and platform_id > 0 group by " + level ;
						result.add(sql);
					}
				}
			}
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return result;
	}
	
	public static List<String> getAndroidMonthDataSql(String day){
		List<String> result = new ArrayList<String>();
		Calendar calendar = Calendar.getInstance();
		try {
			calendar.setTime(DateUtil.simpleDateFormat.parse(day));
			int i = calendar.get(Calendar.DAY_OF_MONTH);
			if(i == 1){
				String yesterday = DateUtil.getYesterday(day);
				int m = calendar.get(Calendar.MONTH);
				if(m==0){
					m=12;
				}
				for(i=m-1; i >= 0; i--){
//				for(i=m-1; i >= 5; i--){
					String[] ms = DateUtil.getPreviousMonthDate(day, m-i);
					if(ms.length > 1){
						String reg_date_1 = ms[0];
						String reg_date_2 = ms[1];
						String sql = "select '" + reg_date_1 + "', platform_id, product_id, '99999999', '"+ m + "', sum(value) from ltv_count where reg_date between '"+reg_date_1+"' and '"+reg_date_2+"' and day = '" + yesterday +"' and type = 65 and product_id > 0 and channel_id > 0 and channel_id != '5' and product_nickname > 0 and platform_id > 0 group by platform_id, product_id" ;
						result.add(sql);
						String sql10 = "select '" + reg_date_1 + "', platform_id, '-1', '99999999', '"+ m + "', sum(value) from ltv_count where reg_date between '"+reg_date_1+"' and '"+reg_date_2+"' and day = '" + yesterday +"' and type = 65 and product_id > 0 and channel_id > 0 and channel_id != '5' and product_nickname > 0 and platform_id > 0 group by platform_id" ;
						result.add(sql10);
					}
				}
			}
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return result;
	}
	
	public static void main(String[] args) {
		String day = "2017-01-31";
		System.out.println(getLtvWeeksSql("platform_id", day));
//		List<String> list = getMonthDataSql("platform_id",day);
//		for (String sql : list) {
//			System.out.println(sql);
//		}
//		list = getAndroidMonthDataSql(day);
//		for (String sql : list) {
//			System.out.println(sql);
//		}
	}
}
