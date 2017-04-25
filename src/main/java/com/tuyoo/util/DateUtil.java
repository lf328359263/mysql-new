package com.tuyoo.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;

public class DateUtil {
	
	public static SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
	
	public static String getDates(String day, Integer[] interval){
		StringBuffer sb= new StringBuffer();
		for (int i : interval) {
			sb.append("'");
			sb.append(getPreviousDate(day, i));
			sb.append("'");
			sb.append(",");
		}
		return sb.substring(0, sb.length()-1);
	}
	
	public static String getPreviousDate(String day, int interval){
		return getNextDate(day, -interval);
	}
	
	public static String getYesterday(String day){
		return getPreviousDate(day, 1);
	}

	public static String getNextDate(String day, int interval){
		Calendar instance = Calendar.getInstance();
		try {
			instance.setTime(simpleDateFormat.parse(day));
			instance.add(Calendar.DATE, interval);
			return simpleDateFormat.format(instance.getTime());
		} catch (ParseException e) {
			e.printStackTrace();
			return "";
		}
	}
	
	/**
	 * 返回月前起止日期
	 * @param day
	 * @param interval
	 * @return
	 */
	public static String[] getPreviousMonthDate(String day, int interval){
		Calendar instance = Calendar.getInstance();
		String[] result = new String[2];
		try {
			instance.setTime(simpleDateFormat.parse(day));
			instance.add(Calendar.MONTH, -interval);
			result[0] = simpleDateFormat.format(instance.getTime());
			instance.add(Calendar.MONTH, 1);
			instance.add(Calendar.DATE, -1);
			result[1] = simpleDateFormat.format(instance.getTime());
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return result;
	}
	
	public static String getPreWeeksDates(String day, int weeks){
		ArrayList<Integer> intervals = new ArrayList<Integer>();
		while(weeks > 0){
			intervals.add(weeks*7);
			weeks--;
		}
		Integer[] ins = intervals.toArray(new Integer[intervals.size()]);
		return getDates(getNextDate(day, 1), ins);
	}
	
	public static void main(String[] args) {
		String day = "2016-10-30";
//		Integer[] ins = {7,14,21,28};
//		String[] date = getPreviousMonthDate(day, 2);
//		System.out.println(date[0]+ "\t" + date[1]);
		System.out.println(getPreWeeksDates(day,12));
	}
}
