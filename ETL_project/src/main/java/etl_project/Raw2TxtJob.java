package etl_project;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.codahale.metrics.Counter;
import com.facebook.fb303.FacebookService.AsyncProcessor.getCounter;

import run_frame.util.JobRunResult;
import run_frame.util.JobRunUtil;

public class Raw2TxtJob extends Configured implements Tool{
	
	@Override
	public int run(String[] args) throws Exception {
		
		Configuration conf = getConf();
		
		
		JobControl jobc = new JobControl("raw2txt");
		
		Raw2Txt raw2Txt = new Raw2Txt();
		
		raw2Txt.setConf(conf);
		
		ControlledJob etljob = raw2Txt.getControlledJob();
		
		
		jobc.addJob(etljob);
		
		JobRunResult result = JobRunUtil.run(jobc);
		
		result.print(false);
		
		
		
		
		return 0;
	}

	public static void main(String[] args) throws Exception {
//		-Dtask.id=hai   -Dtask.input.dir=/tmp/etl/input     -Dtask.base.dir=/tmp/etl/output  			
		System.exit(ToolRunner.run(new Raw2TxtJob(), args));
	}

}
