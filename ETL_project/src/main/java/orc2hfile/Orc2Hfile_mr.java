/**
 * Orc2Hfile_mr.java
 * orc2hfile
 * @author   任超                      
*/

package orc2hfile;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hive.ql.io.orc.OrcNewInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * TODO(这里用一句话描述这个类的作用)
 * <p>
 * TODO(这里描述这个类补充说明 – 可选)
 * @author   任超                     
 * @Date	 2018年10月17日 	 
 */
public class Orc2Hfile_mr extends Configured implements Tool{
	
	
	static SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
	
	static TableName tableName = TableName.valueOf("user_install_status");
	
	static class Orc2HfileMapper extends Mapper<NullWritable, OrcStruct, ImmutableBytesWritable, Put>{

		StructObjectInspector inspector=null;
		
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			String schema = "struct<aid:string,pkgname:string,uptime:bigint,type:int,country:string,gpcategory:string>";
			
			TypeInfo typrinfo = TypeInfoUtils.getTypeInfoFromTypeString(schema);
			
			 inspector = (StructObjectInspector) OrcStruct.createObjectInspector(typrinfo);
			
			
		}
		
		@Override
		protected void map(NullWritable key, OrcStruct orc,Context context)
				throws IOException, InterruptedException {
			String aid = getOrcData(orc, "aid");
			String pkgname = getOrcData(orc, "pkgname");
			String uptime = getOrcData(orc, "uptime");
			String type = getOrcData(orc, "type");
			String country = getOrcData(orc, "country");
			String gpcategory = getOrcData(orc, "gpcategory");
			
			
			if(uptime.equals("null")||aid.equals("null")){
				context.getCounter("fuck","bad line").increment(1L);
				
			}
			long time = Long.parseLong(uptime);
			Date date = new Date(time * 1000);
			uptime=df.format(date);
			
			
			
			System.out.println("aid ="+ aid +"\tpkgname"+pkgname+"\tuptime"+uptime+"\ttype"+type+"\tcountry"+country+"\tgpcategory"+gpcategory);
			
			String rowkey = aid+"_"+uptime;
			
			Put put = new Put(rowkey.getBytes());
			
			if(!pkgname.equals("null")){
				put.addColumn("cf".getBytes(), "pkgname".getBytes(), pkgname.getBytes());
			}
			if(!type.equals("null")){
				put.addColumn("cf".getBytes(), "type".getBytes(), type.getBytes());
			}
			if(!country.equals("null")){
				put.addColumn("cf".getBytes(), "country".getBytes(), country.getBytes());
			}
			if(!gpcategory.equals("null")){
				put.addColumn("cf".getBytes(), "gpcategory".getBytes(), gpcategory.getBytes());
			}
			
			
			ImmutableBytesWritable keyout = new ImmutableBytesWritable(rowkey.getBytes());
			
			context.write(keyout, put);
			
			
			
			
		}
		
		private String getOrcData(OrcStruct orc ,String key){
			
			StructField field = inspector.getStructFieldRef(key);
			String data = String.valueOf(inspector.getStructFieldData(orc, field));
			if(data ==null || data.equals("")|| data.equalsIgnoreCase("null")){
				data="null";
			}
			return data;
			
		}
	
	}
	
	
	
	@Override
	public int run(String[] args) throws Exception {
		
		/*Configuration conf = getConf();
		Job job =Job.getInstance(conf,"orc2hfile");
		
		
		job.setJarByClass(Orc2Hfile_mr.class);
		job.setMapperClass(Orc2HfileMapper.class);
		job.setNumReduceTasks(0);
		
		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		job.setMapOutputValueClass(Put.class);
		
		job.setInputFormatClass(OrcNewInputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		
		
		Path p = new Path(args[1]);
		FileOutputFormat.setOutputPath(job, p);
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(p)){
			fs.delete(p,true);
		}
		
		
		Configuration hbaseConf = HBaseConfiguration.create();
		
		Connection conn = ConnectionFactory.createConnection(hbaseConf);
		HTable table = (HTable) conn.getTable(tableName);
		
		//上面的几行代码，都是围绕该方法写的
		HFileOutputFormat2.configureIncrementalLoad(job, table.getTableDescriptor(), table.getRegionLocator());
		
		
		boolean status = job.waitForCompletion(false);
		return status?0:1;*/
		
		return 0;
		
	}
	
	
/*	public static void main(String[] args) throws Exception {
		
     	file:/C:\tmp\hbase\input   file:/C:\tmp\hbase\output  nn1.hadoop:2181,nn2.hadoop:2181,s1.hadoop:2181
		
		 file:/C:\tmp\hbase\input   file:/C:\tmp\hbase\output  -Dhbase.zookeeper.quorum=nn1.hadoop:2181,nn2.hadoop:2181,s1.hadoop:2181
		
		
		System.exit(ToolRunner.run(new Orc2Hfile_mr(), args));
	}*/

}

