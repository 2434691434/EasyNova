package orc2hfile;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.hadoop.hive.ql.io.orc.OrcNewInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.zookeeper.KeeperException;

import orc2hfile.Orc2Hfile_mr.Orc2HfileMapper;

public class Job_chain extends Configured implements Tool{

	
	
	@Override
	public int run(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		JobControl jobc = new JobControl("orc2hfile_input");
		
		ControlledJob o2f = getO2fJob(args,conf);
		
		jobc.addJob(o2f);
		
		jobc.run();
		return 0;
	}

	private ControlledJob getO2fJob(String[] args, Configuration conf) throws IOException {

		ControlledJob o2f = new ControlledJob(conf);
		Job job =Job.getInstance(conf,"orc2hfile");
		
		
		job.setJarByClass(Orc2Hfile_mr.class);
		job.setMapperClass(Orc2HfileMapper.class);
		
		
		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		job.setMapOutputValueClass(Put.class);
		
		job.setInputFormatClass(OrcNewInputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		
		
		
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		
		
		Configuration hbaseConf = HBaseConfiguration.create();
		
		Connection conn = ConnectionFactory.createConnection(hbaseConf);
		HTable table = (HTable) conn.getTable(Orc2Hfile_mr.tableName);
		
		//上面的几行代码，都是围绕该方法写的
		HFileOutputFormat2.configureIncrementalLoad(job, table.getTableDescriptor(), table.getRegionLocator());
		
		RecoverableZooKeeper keeper = new RecoverableZooKeeper("nn1.hadoop:2181,nn2.hadoop:2181,s1.hadoop:2181", 10000, null, 4, 3, null);
		try {
			keeper.reconnectAfterExpiration();
		} catch (KeeperException e) {
			// TODO 自动生成的 catch 块
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO 自动生成的 catch 块
			e.printStackTrace();
		}
		
		o2f.setJob(job);
		return o2f;
	}
	
	public static void main(String[] args) throws Exception {
		
//		file:/C:\tmp\hbase\input   file:/C:\tmp\hbase\output  nn1.hadoop:2181,nn2.hadoop:2181,s1.hadoop:2181
		/**
		 * file:/C:\tmp\hbase\input   file:/C:\tmp\hbase\output  -Dhbase.zookeeper.quorum=nn1.hadoop:2181,nn2.hadoop:2181,s1.hadoop:2181
		 */
		
		System.exit(ToolRunner.run(new Job_chain(), args));
	}


}
