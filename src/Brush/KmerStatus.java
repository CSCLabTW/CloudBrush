/*
    KmerStatus.java
    2012 Ⓒ CloudBrush, developed by Chien-Chih Chen (rocky@iis.sinica.edu.tw), 
    released under Apache License 2.0 (http://www.apache.org/licenses/LICENSE-2.0) 
    at: https://github.com/ice91/CloudBrush
*/

package Brush;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;


public class KmerStatus extends Configured implements Tool
{
	private static final Logger sLogger = Logger.getLogger(KmerStatus.class);

	public static class KmerStatusMapper extends MapReduceBase
    implements Mapper<LongWritable, Text, Text, IntWritable>
	{
		public void map(LongWritable lineid, Text txt,
				        OutputCollector<Text, IntWritable> output, Reporter reporter)
		                throws IOException
		{
           String vals[] = txt.toString().split("\t");
           output.collect(new Text(vals[1]), new IntWritable(1));
		}
	}

	public static class KmerStatusReducer extends MapReduceBase
	implements Reducer<Text, IntWritable, Text, IntWritable>
	{
		public void reduce(Text prefix, Iterator<IntWritable> iter,
						   OutputCollector<Text, IntWritable> output, Reporter reporter)
						   throws IOException
		{
            int sum =0;
            while(iter.hasNext())
			{
                sum += iter.next().get();
            }
            output.collect(prefix, new IntWritable(sum));
		}
	}



	public RunningJob run(String inputPath, String outputPath) throws Exception
	{
		sLogger.info("Tool name: KmerStatus");
		sLogger.info(" - input: "  + inputPath);
		sLogger.info(" - output: " + outputPath);

		JobConf conf = new JobConf(KmerStatus.class);
		conf.setJobName("KmerStatus " + inputPath + " " + BrushConfig.K);
       // conf.setLong("AllKmer", allkmer);

		BrushConfig.initializeConfiguration(conf);

		FileInputFormat.addInputPath(conf, new Path(inputPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(IntWritable.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
        
        //conf.setBoolean("mapred.output.compress", true);

		conf.setMapperClass(KmerStatusMapper.class);
		conf.setReducerClass(KmerStatusReducer.class);

		//delete the output directory if it exists already
		FileSystem.get(conf).delete(new Path(outputPath), true);

		return JobClient.runJob(conf);
	}

	public int run(String[] args) throws Exception
	{
		String inputPath  = "";
		String outputPath = "";
		BrushConfig.K = 21;

		long starttime = System.currentTimeMillis();

		run(inputPath, outputPath);

		long endtime = System.currentTimeMillis();

		float diff = (float) (((float) (endtime - starttime)) / 1000.0);

		System.out.println("Runtime: " + diff + " s");

		return 0;
	}

	public static void main(String[] args) throws Exception
	{
		int res = ToolRunner.run(new Configuration(), new KmerStatus(), args);
		System.exit(res);
	}
}
