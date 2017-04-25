package com.tuyoo.mysqltest;

import java.text.SimpleDateFormat;
import java.util.Date;

public class DateTrans {
	
	public static void main(String[] args) {
		String event_time = "1483027131";
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat();
		System.out.println(simpleDateFormat.format(new Date(Long.parseLong(event_time+"000"))));
	}
	
}
