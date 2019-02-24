package etl_project;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;




public class Etl_counter extends Configured implements Tool{
	
	
	public static SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
	
	public static long total_num = 0;
	
	public static long country_num = 0;
	
	public static  long user_agent_num = 0;
	
	
	public static class Etl_counterMapper extends Mapper<LongWritable, Text, NullWritable, Text>{
		


		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, NullWritable, Text>.Context context)
				throws IOException, InterruptedException {
			
			context.getCounter("raw_data", "total").increment(1L);

			// context.getCounter("raw", "line num").increment(1L);

			String[] split = value.toString().split("");
			if (split.length == 9) {
				// String local_ip=split[0];
				// String remote_user = split[1];
				String local_time = split[2];
				/**
				 * 获取标准时间
				 */
				String[] split_local_time = local_time.split(" ");
				if (split_local_time.length == 2) {
					String t1 = split_local_time[0];

					String t2 = t1.replace("[", "");
					String time = t2;

					// 30/Oct/2018:17:15:51
					SimpleDateFormat sdf = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.US);
					try {
						Date date = sdf.parse(time);
						SimpleDateFormat sdf2 = new SimpleDateFormat("yyyyMMddHHmmss");
						time = sdf2.format(date);
					} catch (ParseException e) {
						e.printStackTrace();
					}
					// System.out.println(time);

					/**
					 * 获取uid ,country , uip
					 */
					// "GET
					// /?uid=cf4278314ef8e4b996e1b798d8eb92cf&COUNTRY=AU&UIP=1.0.0.2
					// HTTP/1.1"
					// requst 包含uid country uip
					String request = split[3];
					// System.out.println(request);
					String uid = null;
					String country = null;
					String uip = null;

					String[] split_request = request.split("&");
					if (split_request.length == 3) {
						String raw_id = split_request[0];
						String[] split_raw_id = raw_id.split("=");
						if (split_raw_id.length == 2) {
							uid = split_raw_id[1];
//							System.out.println(uid);
							// -----------------------------------
							String raw_country = split_request[1];
							String[] split_raw_country = raw_country.split("=");
							if (split_raw_country.length == 2) {
								country = split_raw_country[1];
								
								context.getCounter("raw_data", "country").increment(1L);
//								System.out.println(country);
								// ------------------------------------
								String raw_ip = split_request[2];
								String[] split_raw_ip = raw_ip.split("=");
								if (split_raw_ip.length == 2) {
									String ip = split_raw_ip[1];
									uip = ip.replace(" HTTP/1.1\"", "");
//									System.out.println(uip);
								}
							}
						}
					}

					/**
					 * 获取status
					 */
					String status = split[4];
					// System.out.println(status);

					/**
					 * 获取body_bytes_sent
					 */
					String body_bytes_sent = split[5];
					// System.out.println(body_bytes_sent);

					// String http_referer = split[6];

					/**
					 * 获取用户代理
					 */
					String http_user_agent = split[7];
					http_user_agent = http_user_agent.replace("\"", "");
					http_user_agent = http_user_agent.replace("\t", "-");
					http_user_agent = http_user_agent.replace(" ", "-");
					
					context.getCounter("raw_data", "user_agent").increment(1L);
					// System.out.println(http_user_agent);
					// System.out.println(http_user_agent);

					// String http_x_forwarded_for = split[8];

					String line_data = uid + "\t" + time + "\t" + uip + "\t" + country + "\t" + status + "\t"
							+ body_bytes_sent + "\t" + http_user_agent + "\t" + "null" + "\t" + "null";
					// System.out.println(line_data);
					Text valueout = new Text();
					valueout.set(line_data);
					context.write(NullWritable.get(), valueout);
				}
			}else if (split.length == 10) {

				// 106.11.156.246-12/Dec/2017:00:17:34 +0800GET
				// /users/12/replies?page=2 HTTP/1.12004421--YisouSpider-
				String local_ip = split[0];
				// System.out.println(local_ip);

				/**
				 * 1空着
				 */

				// String remote_user = split[1];
				String local_time = split[2];
				/**
				 * 获取标准时间
				 */
				String[] split_local_time = local_time.split(" ");
				if (split_local_time.length == 2) {
					String t1 = split_local_time[0];
					String time = t1;
					// 30/Oct/2018:17:15:51
					SimpleDateFormat sdf = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.US);
					try {
						Date date = sdf.parse(time);
						SimpleDateFormat sdf2 = new SimpleDateFormat("yyyyMMddHHmmss");
						time = sdf2.format(date);
					} catch (ParseException e) {
						e.printStackTrace();
					}
					// System.out.println(time);

					// GET /users/12/replies?page=2 HTTP/1.1
					String request = split[3];
					String web_request = null;
					// System.out.println(request);
					String[] spl2 = request.split(" ");
					if (spl2.length == 3) {
						web_request = spl2[1];
					}
					// System.out.println(web_request);

					/**
					 * 获取status
					 */
					String status = split[4];
					System.out.println(status);

					/**
					 * 获取body_bytes_sent
					 */
					String body_bytes_sent = split[5];
					// System.out.println(body_bytes_sent);

					/**
					 * 6空着
					 */

					/**
					 * 获取http_referer
					 */
					String http_referer = split[7];

					/**
					 * 获取用户代理
					 */
					String http_user_agent = split[8];
					// http_user_agent = http_user_agent.replace("\"", "");
					// http_user_agent = http_user_agent.replace("\t", "-");
					// http_user_agent = http_user_agent.replace(" ", "-");
					// System.out.println(http_user_agent);

					// String http_x_forwarded_for = split[8];

					// String line_data = local_ip + "\t" + time + "\t" +
					// web_request + "\t" + status + "\t" + body_bytes_sent +
					// "\t"
					// + http_referer + "\t" + http_user_agent;

					String line_data = "null" + "\t" + time + "\t" + local_ip + "\t" + "null" + "\t" + status + "\t"
							+ body_bytes_sent + "\t" + http_user_agent + "\t" + http_referer + "\t" + web_request;

					// System.out.println(line_data);
					Text valueout = new Text();
					valueout.set(line_data);
					context.write(NullWritable.get(), valueout);
				}

			}

		}
		
	}
	
	
	@Override
	public int run(String[] args) throws Exception {
		

		//获取Configuration对象
		Configuration conf = getConf();
		//创建job对象
		Job job = Job.getInstance(conf, "raw_data_count");
		//设置job参数
		
		//设置job运行类
		job.setJarByClass(Etl_counter.class);
		//设置任务mapper运行类
		job.setMapperClass(Etl_counterMapper.class);
		
		
		job.setNumReduceTasks(0);
		
		//设置任务mapper输出的key的类型
		job.setMapOutputKeyClass(NullWritable.class);
		//设置任务mapper输出的value的类型
		job.setMapOutputValueClass(Text.class);
		
		
		
//		【默认是TextInputFormat.class】如果是文本，可以不写；如果是其他的就必须设置此项
		job.setInputFormatClass(TextInputFormat.class);
		
//		【默认是TextOutputFormat.class】如果是文本，可以不写；如果是其他的就必须设置此项		
		job.setOutputFormatClass(TextOutputFormat.class);
		
		
		//设置任务的输入目录
		FileInputFormat.addInputPath(job, new Path(args[0]));
		
		//输出目录Path对象
		Path outputDir = new Path(args[1]);
		//设置任务的输出目录
		FileOutputFormat.setOutputPath(job, outputDir);
		
		//自动删除输出目录
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(outputDir)){
			//递归删目录
			fs.delete(outputDir, true);
			System.out.println("output dir【" + outputDir.toString() + "】 is deleted");
		}
		
		//运行job任务， 阻塞的方法
		boolean status = job.waitForCompletion(false);
		
		
		Counters counters = job.getCounters();
		CounterGroup group = counters.getGroup("raw_data");
		//遍历组的所有counter
		StringBuilder sb = new StringBuilder();
		sb.append("\n").append(group.getDisplayName());
		for(Counter counter : group){
			sb.append("\n\t").append(counter.getDisplayName()).append("=").append(counter.getValue());
		}
		System.out.println("Counters:\n" + sb.toString());
		
		//可以通过counter名称查询
		 total_num = group.findCounter("total").getValue();
		 country_num = group.findCounter("country").getValue();
		 user_agent_num = group.findCounter("user_agent").getValue();
		System.out.println("total num="+total_num);
		System.out.println("country num="+country_num);
		System.out.println("user agent num="+user_agent_num);
		

			
		
		
		
		DriverManager.deregisterDriver(new com.mysql.jdbc.Driver());
		
		// 2. 建立连接
		Connection conn = DriverManager.getConnection("jdbc:mysql://192.168.88.195:3306/test?user=&password=12345678");
		
		
		
			
			
			
			try {
				
				System.out.println(conn);
				
				Date date = new Date();
				String day = sdf.format(date);
				System.out.println(day);
				
				Long t = total_num;
				
				Long c =country_num;
				
				Long u = user_agent_num;
				
				//String.format("%.2f", ((c.doubleValue() / t.doubleValue()) * 100)) + "%";
				
				Double d = c.doubleValue()/t.doubleValue()*100;
				Double e = u.doubleValue()/t.doubleValue()*100;
//				System.out.println(d);
//				
//				System.out.println(String.valueOf(d).length());	
//				System.out.println(String.valueOf(e).length());
				String coun = null;
				String user = null;
				if(String.valueOf(d).length()>=6 && String.valueOf(e).length()>=6){
				 coun = String.valueOf(d).substring(0, 5)+"%";
				 user = String.valueOf(e).substring(0, 5)+"%";
				}else{
					coun = "0";
					user = "0";
				}
//				
//				System.out.println("country_rate=="+coun+"\t"+"user_agent_rate=="+user);
				
//				insert into 表名 （属性列表）values (值列表1)，(值列表2)，...;
//				String str = "Insert into renchao08_etl values (\"20181030\",100,99,\"99%\",96,\"96%\");";
				String str = "Insert into renchao08_etl values (\""+day+"\","+total_num+","+country_num+",\""+coun+"\","+user_agent_num+",\""+user+"\");";
				Statement statement = conn.createStatement();
				
//				String str = "select * from renchao08_etl;";
				statement.execute(str);
				
				
//				while(resultSet.next()){
//					
//					System.out.println(resultSet.getLong(4));
//				}
				
				
			} catch (SQLException e) {
				e.printStackTrace();
			}finally {
				if(conn!=null){
					try {
						conn.close();
					} catch (SQLException e) {
						e.printStackTrace();
					}
				}
			}
		
		
		
		
		return status ? 0 : 1;
		
	
	}
	
//	public static void main(String[] args) throws Exception {
//		/tmp/etl/input /tmp/etl/output
//		System.exit(ToolRunner.run(new Etl_counter(), args));
//	}
	

	
		public static void main(String[] args) throws Exception {
			
			System.exit(ToolRunner.run(new Etl_counter(), args));
			
		}
		
}
