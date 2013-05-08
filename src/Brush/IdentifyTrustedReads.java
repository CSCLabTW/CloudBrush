/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
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


public class IdentifyTrustedReads extends Configured implements Tool
{
	private static final Logger sLogger = Logger.getLogger(IdentifyTrustedReads.class);

	public static class IdentifyTrustedReadsMapper extends MapReduceBase
    implements Mapper<LongWritable, Text, Text, IntWritable>
	{
		public void map(LongWritable lineid, Text txt,
				        OutputCollector<Text, IntWritable> output, Reporter reporter)
		                throws IOException
		{
           String vals[] = txt.toString().split("\t");
           output.collect(new Text(vals[0]), new IntWritable(Integer.parseInt(vals[1])));
		}
	}

	public static class IdentifyTrustedReadsReducer extends MapReduceBase
	implements Reducer<Text, IntWritable, Text, IntWritable>
	{
        private static long KmerThreshold = 1;
        private static int K = 1;
        private static long Readlen = 36;
        //private static long LowBound = 3;
		public void configure(JobConf job) {
            K = Integer.parseInt(job.get("K"));
            Readlen = Long.parseLong(job.get("READLENGTH"));
            //KmerCov = Long.parseLong(job.get("KmerCov")); 
            //LowBound = Long.parseLong(job.get("LOW_KMER"));
            //KmerCov = (long)Float.parseFloat(job.get("EXPCOV"));
            KmerThreshold = Long.parseLong(job.get("KmerThreshold"));
		}
        
		public void reduce(Text prefix, Iterator<IntWritable> iter,
						   OutputCollector<Text, IntWritable> output, Reporter reporter)
						   throws IOException
		{
            int sum =0;
            int untrust_count = 0;
            int TRUST = 1;
            while(iter.hasNext())
			{
                int frequency = iter.next().get();
                if (frequency <= KmerThreshold) {
                    untrust_count = untrust_count + 1;
                    TRUST = 0;
                    continue;
                }
            }
            /*if (untrust_count >= K/) {
                TRUST = 0;
            }*/
            
            output.collect(prefix, new IntWritable(TRUST));
		}
	}



	public RunningJob run(String inputPath, String outputPath, long kmer_threshold) throws Exception
	{
		sLogger.info("Tool name: IdentifyTrustedReads");
		sLogger.info(" - input: "  + inputPath);
		sLogger.info(" - output: " + outputPath);

		JobConf conf = new JobConf(IdentifyTrustedReads.class);
		conf.setJobName("IdentifyTrustedReads " + inputPath + " " + BrushConfig.K);
        conf.setLong("KmerThreshold", kmer_threshold);
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

		conf.setMapperClass(IdentifyTrustedReads.IdentifyTrustedReadsMapper.class);
		conf.setReducerClass(IdentifyTrustedReads.IdentifyTrustedReadsReducer.class);

		//delete the output directory if it exists already
		FileSystem.get(conf).delete(new Path(outputPath), true);

		return JobClient.runJob(conf);
	}

	public int run(String[] args) throws Exception
	{
		String inputPath  = "/cygdrive/contrail-bio/data/Ec10k.sim.sfa";
		String outputPath = "/cygdrive/contrail-bio/";
		BrushConfig.K = 21;

		long starttime = System.currentTimeMillis();

		run(inputPath, outputPath, 1);

		long endtime = System.currentTimeMillis();

		float diff = (float) (((float) (endtime - starttime)) / 1000.0);

		System.out.println("Runtime: " + diff + " s");

		return 0;
	}

	public static void main(String[] args) throws Exception
	{
		int res = ToolRunner.run(new Configuration(), new IdentifyTrustedReads(), args);
		System.exit(res);
	}
}

