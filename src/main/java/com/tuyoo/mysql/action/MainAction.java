package com.tuyoo.mysql.action;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.tuyoo.util.DateUtil;
import com.tuyoo.util.MysqlUtil;

public class MainAction {
	
	public static String chipSql = "select day, platform_id, case when game_id='9999' and event_id in('10005','10025','10026','10023','10004','10075','10076','10077','10051','10037','15303','15304','15305','10103','15320','15329','10078') then product_id else game_id end as game_id, event_id, chip_type, channel_id, product_nickname, client_id, type, value from chip_count_new where day= '?' and platform_id > 0 and type in (38,39)";
	
	public static void main(String[] args) {
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		String minDay = "2017-04-07";
		String maxDay = "2017-04-06";
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
				if(list.contains("5")){
					exe5(day);
				}
				if(list.contains("6")){
					exe6(day);
				}
				if(list.contains("7")){
					exe7(day);
				}
				if(list.contains("8")){
					exe8(day);
				}
				if(list.contains("9")){
					exe9(day);
				}
				if(list.contains("10")){
					exe10(day);
				}
				if(list.contains("interval_1")){
					interval1(day);
				}
				if(list.contains("interval_2")){
					interval2(day);
				}
				if(list.contains("interval_3")){
					interval3(day);
				}
				if(list.contains("interval_4")){
					interval4(day);
				}
				if(list.contains("interval_10")){
					interval10(day);
				}
				if(list.contains("ltv_month")){
					ltvMonthCount(day);
				}
				if(list.contains("ltv_week_2")){
					ltvWeekCount2(day);
				}
				if(list.contains("ltv_week_3")){
					ltvWeekCount3(day);
				}
				if(list.contains("ltv_week_4")){
					ltvWeekCount4(day);
				}
				if(list.contains("chip_count_1")){
					chipCount1(day);
				}
				if(list.contains("chip_count_2")){
					chipCount2(day);
				}
				if(list.contains("chip_count_3")){
					chipCount3(day);
				}
				if(list.contains("chip_count_4")){
					chipCount4(day);
				}
				if(list.contains("chip_count_5")){
					chipCount5(day);
				}
				calendar_max.add(Calendar.DATE, -1);
				
			}
		} catch (ParseException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 平台- day_count 优化数据
	 * @param day
	 */
	public static void exe1(String day){
		DataTrans dataTrans = new DataTrans();
		
		Map<String, String> base = new HashMap<String, String>();
		
		//导出 平台统计数据
		String platSelect = "select distinct day, platform_id, type, value from base_test where day = '" +day + "' and type in (1,2,8,26,86) and game_id = -1 and product_id = -1 and channel_id = -1 and platform_id > 0 ";
		dataTrans.cache(platSelect, base);
		
		//导出 充值数据
		String paySelect = "select day, platform_id, type, value from pay_count where day = '"+ day + "' and type in (11,12) and platform_id > 0 and product_id = -1 and order_type = -1 and product_channel_id = -1 and prod_id = -1 and game_id = -1";
		dataTrans.cache(MysqlUtil.getConnection_51(), paySelect, base);
		
		//导出 新增-召回用户数据
		String regressSelect = "select day, platform_id, type, sum(value) value from regress_count where day = '"+day+"' and type in ('123','124') and platform_id > 0 and product_id > 0 and game_id = '-1' and channel_id > 0 and product_nickname > 0 group by day, platform_id, type";
		dataTrans.cache(regressSelect, base);
		
		//数据写入
		String replace = "replace into day_count_1 values (?,?,?)";
		dataTrans.writeToNewMysql(replace);
	}
	
	/**
	 * 平台-产品 day_count 优化数据
	 * @param day
	 */
	public static void exe2(String day){
		DataTrans dataTrans = new DataTrans();
		
		//不替换传参
		Map<String, String> base = new HashMap<String, String>();
		
		//导出 包数据， 并将 插件dau类型替换为包dau
		Map<String, String> product = new HashMap<String, String>();
		String productSelect = "select day, platform_id, product_id, type, value from base_test where day = '" + day + "' and type in (1,18,19,96) and game_id = -1 and channel_id = -1 and product_id > 0 and platform_id != 0";
		dataTrans.cache(productSelect, product);
		
//		峰值在线人数
		String maxOnlineSelect = "select '" + day + "', platform_id, product_id, 56, max(account_num) from gdss_v4.game_online where from_unixtime(out_time, '%Y-%m-%d') = '"+day+"' and room_id = 0 group by platform_id, product_id";
		dataTrans.cache(maxOnlineSelect, base);
		
//		平均在线人数
		String avgOnlineSelect = "select '" + day + "', a.platform_id, b.product_id, 57, a.total/b.c from (select platform_id, product_id, sum(account_num) total from gdss_v4.game_online "
				+ "where from_unixtime(out_time, '%Y-%m-%d') = '" + day + "' and room_id = 0 group by platform_id, product_id) a join "
				+ "(select platform_id, product_id, count(1) c from gdss_v4.game_online where from_unixtime(out_time, '%Y-%m-%d') = '"+ day 
				+ "' and room_id = 0 group by platform_id, product_id) b on a.platform_id = b.platform_id and a.product_id = b.product_id";
		dataTrans.cache(avgOnlineSelect, base);
		
//		次插件DAU+激活(数据概况/平台数据[])
		Map<String, String> sumPlugin = new HashMap<String, String>();
		sumPlugin.put("2", "89");
		sumPlugin.put("8", "88");
		String sumPluginSelect = "select day, platform_id, product_id, type, sum(value) value from base_test where day = '" + day + "' and type in (2,8) and game_id > 0 and channel_id = -1 and product_id != game_id and game_id < 9999 and platform_id > 0 and product_id > 0 group by day, platform_id, type, product_id";
		dataTrans.cache(sumPluginSelect, sumPlugin);
		
		//大厅数据
		Map<String, String> productHall = new HashMap<String, String>();
		productHall.put("2", "54");
		productHall.put("8", "58");
		productHall.put("31", "55");
		String hallSelect = "select day, platform_id, product_id, type, value from base_test where day = '" + day + "' and type in (2,8,31) and game_id = 9999 and channel_id = -1 and product_id > 0 and platform_id != 0 and product_nickname = -1";
		dataTrans.cache(hallSelect, productHall);
		
		//导出 插件数据， 并将 激活改为插件激活
		Map<String, String> game = new HashMap<String, String>();
		game.put("2", "9");
		String gameSelect = "select day, platform_id, game_id, type, value from base_test where day = '" + day + "' and type in (2,8,26,32,33,104,105) and product_id = -1 and channel_id = -1 and game_id > 0 and platform_id != 0";
		dataTrans.cache(gameSelect, game);
		
		//导出 包充值数据
		String paySelect = "select day, platform_id, product_id, type, value from pay_count where day = '"+ day + "' and type in (11,12) and platform_id > 0 and product_id > 0 and order_type = -1 and product_channel_id = -1 and prod_id = -1 and game_id = -1";
		dataTrans.cache(MysqlUtil.getConnection_51(), paySelect, base);
		
		//导出 新增-召回用户数据
		String regressSelect = "select day, platform_id, product_id, type, sum(value) value from regress_count where day = '"+day+"' and type in ('123','124') and platform_id > 0 and product_id > 0 and game_id = '-1' and channel_id > 0 and product_nickname > 0 group by day, platform_id, product_id, type";
		dataTrans.cache(regressSelect, base);
		
		//主包住插件数据（DAU、激活）
		Map<String, String> mainGameMap = new HashMap<String, String>();
		mainGameMap.put("8", "51");
		mainGameMap.put("2", "52");
		String productAndGameSelect = "select day, platform_id, product_id, type, value from base_test where day = '" + day + "' and type in (2,8) and platform_id > 0 and product_id > 0 and game_id = product_id and channel_id = -1";
		dataTrans.cache(productAndGameSelect, mainGameMap);
		
		//数据写入
		String replace = "replace into day_count_2 values (?,?,?,?)";
		dataTrans.writeToNewMysql(replace);
	}
	
	/**
	 * 平台-产品-渠道 day_count 优化数据
	 * @param day
	 */
	public static void exe3(String day){
		DataTrans dataTrans = new DataTrans();
		
		//不替换传参
		Map<String, String> base = new HashMap<String, String>();
		
		//导出 包 大厅数据
		Map<String, String> productHall = new HashMap<String, String>();
		productHall.put("2", "54");
		productHall.put("8", "58");
		productHall.put("31", "55");
		String productHallSelect = "select day, platform_id, product_id, channel_id, type, value from base_test where day = '" + day + "' and type in (2,8,31) and game_id = 9999 and channel_id > 0 and product_id > 0 and platform_id != 0 and product_nickname = -1";
		dataTrans.cache(productHallSelect, productHall);
		
		// 导出包 数据
		String productSelect = "select day, platform_id, product_id, channel_id, type, value from base_test where day = '" + day + "' and type in (1,18,19,96,101) and game_id = -1 and channel_id > 0 and product_id > 0 and platform_id != 0 and product_nickname = -1";
		dataTrans.cache(productSelect, base);
		
		// 导出安卓注册数
		String androidSelect = "select day, platform_id, product_id, '99999999', type, sum(value) value from base_test where day = '" + day + "' and type in (1) and game_id = -1 and channel_id > 0 and product_id > 0 and platform_id > 0 and product_nickname = -1 and channel_id != 5 group by day, platform_id, product_id, type";
		dataTrans.cache(androidSelect, base);
		
		//导出 包充值数据
		String paySelect = "select day, platform_id, product_id, product_channel_id, type, value from pay_count where day = '"+ day + "' and type in (11,12) and platform_id > 0 and product_id > 0 and order_type = -1 and product_channel_id > 0 and product_nickname_id = -1 and prod_id = -1 and game_id = -1";
		dataTrans.cache(MysqlUtil.getConnection_51(), paySelect, base);
		
		//导出 新增 召回用户数
		String regressSelect = "select day, platform_id, product_id, channel_id, type, sum(value) value from regress_count where day = '"+day+"' and type in ('123','124') and platform_id > 0 and product_id > 0 and game_id = '-1' and channel_id > 0 and product_nickname > 0 group by day, platform_id, product_id, channel_id, type";
		dataTrans.cache(regressSelect, base);
		
		//导出主包主插件数据
		Map<String, String> mainGame = new HashMap<String, String>();
		mainGame.put("2", "52");
		mainGame.put("31", "53");
		String mainGameSelect = "select day, platform_id, product_id, channel_id, type, value from base_test where day = '" + day + "' and type in (2,25,26,31,32) and game_id = product_id and channel_id > 0 and product_id > 0 and platform_id != 0 and product_nickname = -1";
		dataTrans.cache(mainGameSelect, mainGame);
		
		//数据写入
		String replace = "replace into day_count_3 values (?,?,?,?,?)";
		dataTrans.writeToNewMysql(replace);
	}
	
	public static void exe10(String day) {
		DataTrans dataTrans = new DataTrans();
		
		//不替换传参
		Map<String, String> base = new HashMap<String, String>();
		
		//导出 包 大厅数据
		Map<String, String> productHall = new HashMap<String, String>();
		productHall.put("2", "54");
		productHall.put("8", "58");
		productHall.put("31", "55");
		String productHallSelect = "select day, platform_id, channel_id, type, value from base_test where day = '" + day + "' and type in (2,8,31) and game_id = 9999 and channel_id > 0 and product_id = -1 and platform_id != 0 and product_nickname = -1";
		dataTrans.cache(productHallSelect, productHall);
		
		// 导出包 数据
		String productSelect = "select day, platform_id, channel_id, type, value from base_test where day = '" + day + "' and type in (1,18,19,96,101) and game_id = -1 and channel_id > 0 and product_id = -1 and platform_id != 0 and product_nickname = -1";
		dataTrans.cache(productSelect, base);
		
		// 导出安卓注册数
		String androidSelect = "select day, platform_id, '99999999', type, sum(value) value from base_test where day = '" + day + "' and type in (1) and game_id = -1 and channel_id > 0 and product_id = -1 and platform_id > 0 and product_nickname = -1 and channel_id != 5 group by day, platform_id, type";
		dataTrans.cache(androidSelect, base);
		
		//导出 包充值数据
		String paySelect = "select day, platform_id, product_channel_id, type, value from pay_count where day = '"+ day + "' and type in (11,12) and platform_id > 0 and product_id = -1 and order_type = -1 and product_channel_id > 0 and product_nickname_id = -1 and prod_id = -1 and game_id = -1";
		dataTrans.cache(MysqlUtil.getConnection_51(), paySelect, base);
		
		//导出主包主插件数据
		Map<String, String> mainGame = new HashMap<String, String>();
		mainGame.put("2", "52");
		mainGame.put("31", "53");
		String mainGameSelect = "select day, platform_id, channel_id, type, value from base_test where day = '" + day + "' and type in (2,25,26,31,32) and game_id = product_id and channel_id > 0 and product_id = -1 and platform_id > 0 and product_nickname = -1";
		dataTrans.cache(mainGameSelect, mainGame);
		
		//数据写入
		String replace = "replace into day_count_10 values (?,?,?,?)";
		dataTrans.writeToNewMysql(replace);
	}
	
	/**
	 * 平台-产品-渠道-子渠道 day_count 优化数据
	 * @param day
	 */
	public static void exe4(String day){
		DataTrans dataTrans = new DataTrans();
		
		//不替换传参
		Map<String, String> base = new HashMap<String, String>();
		
		//导出 包 大厅数据
		Map<String, String> productHall = new HashMap<String, String>();
		productHall.put("2", "54");
		productHall.put("8", "58");
		productHall.put("31", "55");
		String productHallSelect = "select day, platform_id, product_id, channel_id, product_nickname, type, value from base_test where day = '" + day + "' and type in (2,8,31) and game_id = 9999 and channel_id > 0 and product_id > 0 and platform_id != 0 and product_nickname > 0 and client_id = -1";
		dataTrans.cache(productHallSelect, productHall);
		
		// 导出包 数据
		String productSelect = "select day, platform_id, product_id, channel_id, product_nickname, type, value from base_test where day = '" + day + "' and type in (1,18,19,96,101) and game_id = -1 and channel_id > 0 and product_id > 0 and platform_id != 0 and product_nickname > 0 and client_id = -1";
		dataTrans.cache(productSelect, base);
		
		// 导出新增 召回用户数
		String regressSelect = "select day, platform_id, product_id, channel_id, product_nickname, type, sum(value) value from regress_count where day = '"+day+"' and type in ('123','124') and platform_id > 0 and product_id > 0 and game_id = '-1' and channel_id > 0 and product_nickname > 0 group by day, platform_id, product_id, channel_id, product_nickname, type";
		dataTrans.cache(regressSelect, base);
		
		//导出 包充值数据
		String paySelect = "select day, platform_id, product_id, product_channel_id, product_nickname_id, type, value from pay_count where day = '"+ day + "' and type in (11,12) and platform_id > 0 and product_id > 0 and order_type = -1 and product_channel_id > 0 and product_nickname_id > 0 and client_id = -1 and prod_id = -1 and game_id = -1";
		dataTrans.cache(MysqlUtil.getConnection_51(), paySelect, base);
		
		//导出主包主插件数据
		Map<String, String> mainGame = new HashMap<String, String>();
		mainGame.put("2", "52");
		mainGame.put("31", "53");
		String mainGameSelect = "select day, platform_id, product_id, channel_id, product_nickname, type, value from base_test where day = '" + day + "' and type in (2,25,26,31,32) and game_id = product_id and channel_id > 0 and product_id > 0 and platform_id != 0 and product_nickname > 0 and client_id = -1";
		dataTrans.cache(mainGameSelect, mainGame);
		
		//数据写入
		String replace = "replace into day_count_4 values (?,?,?,?,?,?)";
		dataTrans.writeToNewMysql(replace);
	}
	
	/**
	 * 平台-产品-渠道-子渠道-clientid day_count 优化数据
	 * @param day
	 */
	public static void exe5(String day){
		DataTrans dataTrans = new DataTrans();
		
		//不替换传参
		Map<String, String> base = new HashMap<String, String>();
		
		//导出 包 大厅数据
		Map<String, String> productHall = new HashMap<String, String>();
		productHall.put("2", "54");
		productHall.put("8", "58");
		productHall.put("31", "55");
		String productHallSelect = "select day, platform_id, product_id, channel_id, product_nickname, client_id, type, value from base_test where day = '" + day + "' and type in (2,8,31) and game_id = 9999 and channel_id > 0 and product_id > 0 and platform_id != 0 and product_nickname > 0 and client_id > 0";
		dataTrans.cache(productHallSelect, productHall);
		
		// 导出包 数据
		String productSelect = "select day, platform_id, product_id, channel_id, product_nickname, client_id, type, value from base_test where day = '" + day + "' and type in (1,18,19,96,101) and game_id = -1 and channel_id > 0 and product_id > 0 and platform_id != 0 and product_nickname > 0 and client_id > 0";
		dataTrans.cache(productSelect, base);
		
		//导出 包充值数据
		String paySelect = "select day, platform_id, product_id, product_channel_id, product_nickname_id, client_id, type, value from pay_count where day = '"+ day + "' and type in (11,12) and platform_id > 0 and product_id > 0 and order_type = -1 and product_channel_id > 0 and product_nickname_id > 0 and client_id > 0 and prod_id = -1 and game_id = -1";
		dataTrans.cache(MysqlUtil.getConnection_51(), paySelect, base);
		
		//导出主包主插件数据
		Map<String, String> mainGame = new HashMap<String, String>();
		mainGame.put("2", "52");
		mainGame.put("31", "53");
		String mainGameSelect = "select day, platform_id, product_id, channel_id, product_nickname, client_id, type, value from base_test where day = '" + day + "' and type in (2,25,26,31,32) and game_id = product_id and channel_id > 0 and product_id > 0 and platform_id != 0 and product_nickname > 0 and client_id > 0";
		dataTrans.cache(mainGameSelect, mainGame);
		
		//数据写入
		String replace = "replace into day_count_5 values (?,?,?,?,?,?,?)";
		dataTrans.writeToNewMysql(replace);
	}
	
	/**
	 * 平台-产品-插件 day_count 优化数据
	 * @param day
	 */
	public static void exe6(String day){
		DataTrans dataTrans = new DataTrans();
		
		Map<String, String> map = new HashMap<String, String>();
		String select = "select day, platform_id, product_id, game_id, type, value from base_test where day = '" + day + "' and type in (2,8,23,24) and game_id > 0 and channel_id = -1 and product_id > 0 and platform_id > 0";
		dataTrans.cache(select, map);
		
		String replace = "replace into day_count_6 values (?,?,?,?,?)";
		dataTrans.writeToNewMysql(replace);
	}
	
	/**
	 * 平台-产品-插件-渠道 day_count 优化数据
	 * @param day
	 */
	public static void exe7(String day){
		DataTrans dataTrans = new DataTrans();
		
		Map<String, String> map = new HashMap<String, String>();
		String select = "select day, platform_id, product_id, game_id, channel_id, type, value from base_test where day = '" + day + "' and type in (2,8,23,24) and game_id > 0 and channel_id > 0 and product_id > 0 and platform_id > 0 and product_nickname = -1";
		dataTrans.cache(select, map);
		
		String replace = "replace into day_count_7 values (?,?,?,?,?,?)";
		dataTrans.writeToNewMysql(replace);
	}
	
	/**
	 * 平台-产品-插件-渠道-子渠道 day_count 优化数据
	 * @param day
	 */
	public static void exe8(String day){
		DataTrans dataTrans = new DataTrans();
		
		Map<String, String> map = new HashMap<String, String>();
		String select = "select day, platform_id, product_id, game_id, channel_id, product_nickname, type, value from base_test where day = '" + day + "' and type in (2,8,23,24) and game_id > 0 and channel_id > 0 and product_id > 0 and platform_id > 0 and product_nickname > 0 and client_id = -1";
		dataTrans.cache(select, map);
		
		String replace = "replace into day_count_8 values (?,?,?,?,?,?,?)";
		dataTrans.writeToNewMysql(replace);
	}
	
	/**
	 * 平台-产品-插件-渠道-子渠道-clientid day_count 优化数据
	 * @param day
	 */
	public static void exe9(String day){
		DataTrans dataTrans = new DataTrans();
		
		Map<String, String> map = new HashMap<String, String>();
		String select = "select day, platform_id, product_id, game_id, channel_id, product_nickname, client_id, type, value from base_test where day = '" + day + "' and type in (2,8,23,24) and game_id > 0 and channel_id > 0 and product_id > 0 and platform_id > 0 and product_nickname > 0 and client_id > 0";
		dataTrans.cache(select, map);
		
		String replace = "replace into day_count_9 values (?,?,?,?,?,?,?,?)";
		dataTrans.writeToNewMysql(replace);
	}

	public static void interval1(String day){
		DataTrans dataTrans = new DataTrans();
		Map<String, String> base = new HashMap<String, String>();
		
		//LTV日数据
		String select = IntervalAction.getCycleDataSql("platform_id", day);
		dataTrans.cache(select, base);

		String replace = "replace into interval_count_1 values (?,?,?,?)";
		dataTrans.writeToNewMysql(replace);
	}
	
	public static void interval2(String day){
		DataTrans dataTrans = new DataTrans();
		Map<String, String> base = new HashMap<String, String>();
		
		//LTV日数据
		String select = IntervalAction.getCycleDataSql("platform_id, product_id", day);
		dataTrans.cache(select, base);

		String replace = "replace into interval_count_2 values (?,?,?,?,?)";
		dataTrans.writeToNewMysql(replace);
	}
	
	public static void interval3(String day){
		DataTrans dataTrans = new DataTrans();
		Map<String, String> base = new HashMap<String, String>();
		
		//LTV日数据
		String select = IntervalAction.getCycleDataSql("platform_id, product_id, channel_id", day);
		dataTrans.cache(select, base);
		
		//安卓渠道汇总
		String reg_date = DateUtil.getDates(day, IntervalAction.ltvIntervals);
		String selectAndroid = "select reg_date, platform_id, product_id, '99999999', DATEDIFF(day, reg_date) intervals, type, sum(value) from ltv_count where reg_date in ("+ reg_date+") and day = '"+ day +"' and type = 65 and product_id > 0 and channel_id > 0 and channel_id != '5' and product_nickname > 0 and platform_id > 0 group by reg_date, platform_id, product_id, intervals, type";
		dataTrans.cache(selectAndroid, base);
		
		String replace = "replace into interval_count_3 values (?,?,?,?,?,?)";
		dataTrans.writeToNewMysql(replace);
	}
	
	public static void interval4(String day){
		DataTrans dataTrans = new DataTrans();
		Map<String, String> base = new HashMap<String, String>();
		
		//LTV日数据
		String select = IntervalAction.getCycleDataSql4("platform_id, product_id, channel_id, product_nickname", day);
		dataTrans.cache(select, base);
		
		String replace = "replace into interval_count_4 values (?,?,?,?,?,?,?)";
		dataTrans.writeToNewMysql(replace);
	}
	
	public static void interval10(String day){
		DataTrans dataTrans = new DataTrans();
		Map<String, String> base = new HashMap<String, String>();
		
		//LTV日数据
		String select = IntervalAction.getCycleDataSql("platform_id, channel_id", day);
		dataTrans.cache(select, base);
		
		//安卓渠道汇总
		String reg_date = DateUtil.getDates(day, IntervalAction.ltvIntervals);
		String selectAndroid = "select reg_date, platform_id, '99999999', DATEDIFF(day, reg_date) intervals, type, sum(value) from ltv_count where reg_date in ("+ reg_date+") and day = '"+ day +"' and type = 65 and product_id > 0 and channel_id > 0 and channel_id != '5' and product_nickname > 0 and platform_id > 0 group by reg_date, platform_id, intervals, type";
		dataTrans.cache(selectAndroid, base);
		
		String replace = "replace into interval_count_10 values (?,?,?,?,?)";
		dataTrans.writeToNewMysql(replace);
	}
	
	/**
	 * LTV周统计 平台-产品
	 */
	public static void ltvWeekCount2(String day){
		DataTrans dataTrans = new DataTrans();
		MySQLAction mySQLAction = new MySQLAction(MysqlUtil.getOldConnection());
		
		String selectMonthLtv = IntervalAction.getLtvWeeksSql("platform_id, product_id", day);
		dataTrans.persistResultSetToMysql(mySQLAction.executeQuery(selectMonthLtv), "ltv_week_count_2");
		
		mySQLAction.close();
	}
	
	/**
	 * LTV周统计  平台-产品-渠道
	 */
	public static void ltvWeekCount3(String day){
		DataTrans dataTrans = new DataTrans();
		MySQLAction mySQLAction = new MySQLAction(MysqlUtil.getOldConnection());
		
		String selectMonthLtv = IntervalAction.getLtvWeeksSql("platform_id, product_id, channel_id", day);
		dataTrans.persistResultSetToMysql(mySQLAction.executeQuery(selectMonthLtv), "ltv_week_count_3");
		
		mySQLAction.close();
	}
	
	/**
	 * LTV周统计  平台-产品-渠道
	 */
	public static void ltvWeekCount4(String day){
		DataTrans dataTrans = new DataTrans();
		MySQLAction mySQLAction = new MySQLAction(MysqlUtil.getOldConnection());
		
		String selectMonthLtv = IntervalAction.getLtvWeeksSql("platform_id, product_id, channel_id, product_nickname", day);
		dataTrans.persistResultSetToMysql(mySQLAction.executeQuery(selectMonthLtv), "ltv_week_count_4");
		
		mySQLAction.close();
	}
	
	/**
	 * LTV月统计
	 * @param day
	 */
	public static void ltvMonthCount(String day){
		DataTrans dataTrans = new DataTrans();
		MySQLAction mySQLAction = new MySQLAction(MysqlUtil.getOldConnection());

		List<String> monthDataSql = IntervalAction.getMonthDataSql("platform_id, -1, -1", day);
		if(monthDataSql != null){
			for (String selectMonthLtv : monthDataSql) {
				dataTrans.persistResultSetToMysql(mySQLAction.executeQuery(selectMonthLtv), "ltv_month_count");
			}
		}
		
		monthDataSql = IntervalAction.getMonthDataSql("platform_id, product_id, -1", day);
		if(monthDataSql != null){
			for (String selectMonthLtv : monthDataSql) {
				dataTrans.persistResultSetToMysql(mySQLAction.executeQuery(selectMonthLtv), "ltv_month_count");
			}
		}
		
		monthDataSql = IntervalAction.getMonthDataSql("platform_id, product_id, channel_id", day);
		if(monthDataSql != null){
			for (String selectMonthLtv : monthDataSql) {
				dataTrans.persistResultSetToMysql(mySQLAction.executeQuery(selectMonthLtv), "ltv_month_count");
			}
		}
		
		monthDataSql = IntervalAction.getMonthDataSql("platform_id, -1, channel_id", day);
		if(monthDataSql != null){
			for (String selectMonthLtv : monthDataSql) {
				dataTrans.persistResultSetToMysql(mySQLAction.executeQuery(selectMonthLtv), "ltv_month_count");
			}
		}
		
		monthDataSql = IntervalAction.getAndroidMonthDataSql(day);
		if(monthDataSql != null){
			for (String selectMonthLtv : monthDataSql) {
				dataTrans.persistResultSetToMysql(mySQLAction.executeQuery(selectMonthLtv), "ltv_month_count");
			}
		}
		
		mySQLAction.close();
	}
	
	/*
	 * 金流 非道具统计数据
	 */
	public static void chipCount1(String day){
		DataTrans dataTrans = new DataTrans();
		
		Map<String, String> base = new HashMap<String, String>();
		
		//导出 金流统计数据
		String platSelect = "select day, platform_id, chip_type, type, event_id, sum(value) value from ("+chipSql.replaceAll("\\?", day)+") tmp where chip_type != '6' group by day, platform_id, chip_type, type, event_id";
		dataTrans.cache(platSelect, base);
		
		//数据写入
		String replace = "replace into chip_count_1 values (?,?,?,?,?)";
		dataTrans.writeToNewMysql(replace);
	}
	
	/**
	 * 金流 非道具统计数据
	 * @param day
	 */
	public static void chipCount2(String day){
		DataTrans dataTrans = new DataTrans();
		
		Map<String, String> base = new HashMap<String, String>();
		
		//导出 金流统计数据
		String platSelect = "select day, platform_id, game_id, chip_type, type, event_id, sum(value) value from ("+chipSql.replaceAll("\\?", day)+") tmp where chip_type != '6' group by day, platform_id, game_id, chip_type, type, event_id";
		dataTrans.cache(platSelect, base);
		
		//数据写入
		String replace = "replace into chip_count_2 values (?,?,?,?,?,?)";
		dataTrans.writeToNewMysql(replace);
	}
	
	/**
	 * 金流 非道具统计数据
	 * @param day
	 */
	public static void chipCount3(String day){
		DataTrans dataTrans = new DataTrans();
		
		Map<String, String> base = new HashMap<String, String>();
		
		//导出 金流统计数据
		String platSelect = "select day, platform_id, game_id, channel_id, chip_type, type, event_id, sum(value) value from ("+chipSql.replaceAll("\\?", day)+") tmp where chip_type != '6' group by day, platform_id, game_id, channel_id, chip_type, type, event_id";
		dataTrans.cache(platSelect, base);
		
		//数据写入
		String replace = "replace into chip_count_3 values (?,?,?,?,?,?,?)";
		dataTrans.writeToNewMysql(replace);
	}
	
	
	/**
	 * 金流 非道具统计数据
	 * @param day
	 */
	public static void chipCount4(String day){
		DataTrans dataTrans = new DataTrans();
		
		Map<String, String> base = new HashMap<String, String>();
		
		//导出 金流统计数据
		String platSelect = "select day, platform_id, game_id, channel_id, product_nickname, chip_type, type, event_id, sum(value) value from ("+chipSql.replaceAll("\\?", day)+") tmp where chip_type != '6' group by day, platform_id, game_id, channel_id, product_nickname, chip_type, type, event_id";
		dataTrans.cache(platSelect, base);
		
		//数据写入
		String replace = "replace into chip_count_4 values (?,?,?,?,?,?,?,?)";
		dataTrans.writeToNewMysql(replace);
	}
	
	/**
	 * 金流 非道具统计数据
	 * @param day
	 */
	public static void chipCount5(String day){
		DataTrans dataTrans = new DataTrans();
		
		Map<String, String> base = new HashMap<String, String>();
		
		//导出 金流统计数据
		String platSelect = "select day, platform_id, game_id, channel_id, product_nickname, client_id, chip_type, type, event_id, sum(value) value from ("+chipSql.replaceAll("\\?", day)+") tmp where chip_type != '6' group by day, platform_id, game_id, channel_id, product_nickname, client_id, chip_type, type, event_id";
		dataTrans.cache(platSelect, base);
		
		//数据写入
		String replace = "replace into chip_count_5 values (?,?,?,?,?,?,?,?,?)";
		dataTrans.writeToNewMysql(replace);
	}

}