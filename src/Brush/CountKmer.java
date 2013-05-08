/*
    CountKmer.java
    2012 Ⓒ CloudBrush, developed by Chien-Chih Chen (rocky@iis.sinica.edu.tw), 
    released under Apache License 2.0 (http://www.apache.org/licenses/LICENSE-2.0) 
    at: https://github.com/ice91/CloudBrush*/

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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
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


public class CountKmer extends Configured implements Tool
{
	private static final Logger sLogger = Logger.getLogger(MatchPrefix.class);

	public static class CountKmerMapper extends MapReduceBase
    implements Mapper<LongWritable, Text, Text, Text>
	{
		public static int K = 0;
		public static int TRIM5 = 0;
		public static int TRIM3 = 0;

		public void configure(JobConf job)
		{
			K = Integer.parseInt(job.get("K"));
		}

		public void map(LongWritable lineid, Text nodetxt,
				        OutputCollector<Text, Text> output, Reporter reporter)
		                throws IOException
		{
            Node node = new Node();
			node.fromNodeMsg(nodetxt.toString());
            if (!node.hasCustom("n")){
                reporter.incrCounter("Brush", "nodes", 1);
                //slide the K-mer windows for each read in both strands
                int end = node.len() - K;
                for (int i = 0; i < end; i++)
                {
                    String window_tmp = node.str().substring(i,   i+K);
                    String window_r_tmp = Node.rc(node.str().substring(node.len() - K - i, node.len() - i));
                    //String window_r_tmp = Node.rc(window_tmp);
                    String window = Node.str2dna(window_tmp);
                    String window_r = Node.str2dna(window_r_tmp);
                    //output.collect(new Text(window), new IntWritable((int)node.cov()));
                    //output.collect(new Text(window_r), new IntWritable((int)node.cov()));
                    output.collect(new Text(window), new Text((int)node.cov() + "|" + node.getNodeId()));
                    output.collect(new Text(window_r), new Text((int)node.cov() + "|" + node.getNodeId()));
                    reporter.incrCounter("Brush", "Allkmer", (int)node.cov());
                }
            }
		}
	}

	public static class CountKmerReducer extends MapReduceBase
	implements Reducer<Text, Text, Text, Text>
	{
		private static int K = 0;
        //private static int OVALSIZE = 0;
        private static int All_Kmer = 0;

		public void configure(JobConf job) {
			K = Integer.parseInt(job.get("K"));
		}

		public void reduce(Text prefix, Iterator<Text> iter,
						   OutputCollector<Text, Text> output, Reporter reporter)
						   throws IOException
		{
            int sum =0;
            int read_count = 0;
            List<String> ReadID_list = new ArrayList<String>();
            while(iter.hasNext())
			{
                String msg = iter.next().toString();
				String [] vals = msg.split("\\|");
                sum += Integer.parseInt(vals[0]);
                if (ReadID_list.contains(vals[1])){
                    // do nothing
                } else {
                    read_count = read_count + 1;
                    ReadID_list.add(vals[1]);
                }
            }
            output.collect(new Text(prefix.toString()), new Text(sum+""));
            if (sum >= 2){
                reporter.incrCounter("Brush", "Diffkmer", 1);
            }
		}
	}



	public RunningJob run(String inputPath, String outputPath) throws Exception
	{
		sLogger.info("Tool name: CountKmer");
		sLogger.info(" - input: "  + inputPath);
		sLogger.info(" - output: " + outputPath);

		JobConf conf = new JobConf(CountKmer.class);
		conf.setJobName("CountKmer " + inputPath + " " + BrushConfig.K);

		BrushConfig.initializeConfiguration(conf);

		FileInputFormat.addInputPath(conf, new Path(inputPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		conf.setMapOutputKeyClass(Text.class);
		//conf.setMapOutputValueClass(IntWritable.class);
        conf.setMapOutputValueClass(Text.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
        
        //conf.setBoolean("mapred.output.compress", true);

		conf.setMapperClass(CountKmerMapper.class);
		conf.setReducerClass(CountKmerReducer.class);

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
		int res = ToolRunner.run(new Configuration(), new CountKmer(), args);
		System.exit(res);
	}
}
