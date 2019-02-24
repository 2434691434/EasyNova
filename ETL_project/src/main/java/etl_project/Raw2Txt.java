package etl_project;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import run_frame.base.BaseMR;

public class Raw2Txt extends BaseMR {

	public static class Raw2TxtMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, NullWritable, Text>.Context context)
				throws IOException, InterruptedException {

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
							System.out.println(uid);
							// -----------------------------------
							String raw_country = split_request[1];
							String[] split_raw_country = raw_country.split("=");
							if (split_raw_country.length == 2) {
								country = split_raw_country[1];
								System.out.println(country);
								// ------------------------------------
								String raw_ip = split_request[2];
								String[] split_raw_ip = raw_ip.split("=");
								if (split_raw_ip.length == 2) {
									String ip = split_raw_ip[1];
									uip = ip.replace(" HTTP/1.1\"", "");
									System.out.println(uip);
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
					// System.out.println(http_user_agent);
					// System.out.println(http_user_agent);

					// String http_x_forwarded_for = split[8];

					// String line_data = uid + "\t" + time + "\t" + uip + "\t"
					// + country + "\t" + status + "\t"
					// + body_bytes_sent + "\t" + http_user_agent;

					String line_data = uid + "\t" + time + "\t" + uip + "\t" + country + "\t" + status + "\t"
							+ body_bytes_sent + "\t" + http_user_agent + "\t" + "null" + "\t" + "null";
					// System.out.println(line_data);
					Text valueout = new Text();
					valueout.set(line_data);
					context.write(NullWritable.get(), valueout);
				}

				/**
				 * 以下是清洗的log文件部分
				 */
			} else if (split.length == 10) {

				// 106.11.156.246-12/Dec/2017:00:17:34 +0800GET
				// /users/12/replies?page=2 HTTP/1.12004421--YisouSpider-
				String local_ip = split[0];
				// System.out.println(local_ip);

				

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

	public static class Raw2TxtReducer extends Reducer<NullWritable, Text, NullWritable, Text> {

		Text valueout = new Text();

		@Override
		protected void reduce(NullWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			// context.getCounter("raw", "line type num").increment(1L);
			for (Text text : values) {
				String str = text.toString();
				// System.out.println(str);
				valueout.set(str);
				context.write(NullWritable.get(), valueout);

			}
		}

	}

	@Override
	public Job getJob(Configuration conf) throws Exception {
		Job job = Job.getInstance(conf, getJobNameWithTaskId());

		job.setJarByClass(Raw2Txt.class);
		job.setMapperClass(Raw2TxtMapper.class);
		job.setReducerClass(Raw2TxtReducer.class);

		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, getInputPath());

		FileOutputFormat.setOutputPath(job, getOutputPath(getJobNameWithTaskId()));

		return job;
	}

	@Override
	public String getJobName() {
		// TODO 自动生成的方法存根
		return "raw2txt";
	}

}
