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


public class TagTrustedReads extends Configured implements Tool
{
	private static final Logger sLogger = Logger.getLogger(TagTrustedReads.class);

	public static class TagTrustedReadsMapper extends MapReduceBase
    implements Mapper<LongWritable, Text, Text, Text>
	{
		private static Node node = new Node();
        public void map(LongWritable lineid, Text txt,
				        OutputCollector<Text, Text> output, Reporter reporter)
		                throws IOException
		{
           String vals[] = txt.toString().split("\t");
           if (vals[1].equals(Node.NODEMSG)){
               node.fromNodeMsg(txt.toString());
               output.collect(new Text(node.getNodeId()), new Text(node.toNodeMsg()));
           } else {
               output.collect(new Text(vals[0]), new Text(Node.UPDATEMSG + "\t" + vals[1]));
           }
           //output.collect(new Text(vals[0]), new IntWritable(Integer.parseInt(vals[1])));
		}
	}

	public static class TagTrustedReadsReducer extends MapReduceBase
	implements Reducer<Text, Text, Text, Text>
	{
        private static long KmerCov = 0;
        private static long LowBound = 3;
		public void configure(JobConf job) {
            //KmerCov = Long.parseLong(job.get("KmerCov")); 
            KmerCov = (long)Float.parseFloat(job.get("EXPCOV"));
		}
        
		public void reduce(Text prefix, Iterator<Text> iter,
						   OutputCollector<Text, Text> output, Reporter reporter)
						   throws IOException
		{
            Node node = new Node(prefix.toString());
            int sawnode = 0;
            boolean trust = false;
            while(iter.hasNext())
			{
                String msg = iter.next().toString();
                String [] vals = msg.split("\t");

				if (vals[0].equals(Node.NODEMSG))
				{
					node.parseNodeMsg(vals, 0);
					sawnode++;
				}
				else if (vals[0].equals(Node.UPDATEMSG))
				{
					if (vals[1].equals("1")) {
                        trust = true;
                    } else if (vals[1].equals("0")) {
                        trust = false;
                    }
				}
				else
				{
					throw new IOException("Unknown msgtype: " + msg);
				}
            }
            if (trust) {
                node.setisUnique(false);
            } else {
                node.setisUnique(true);
                reporter.incrCounter("Brush", "failed_reads", 1);  
            }
            output.collect(prefix, new Text(node.toNodeMsg()));
		}
	}



	public RunningJob run(String inputPath, String outputPath) throws Exception
	{
		sLogger.info("Tool name: TagTrustedReads");
		sLogger.info(" - input: "  + inputPath);
		sLogger.info(" - output: " + outputPath);

		JobConf conf = new JobConf(TagTrustedReads.class);
		conf.setJobName("TagTrustedReads " + inputPath + " " + BrushConfig.K);
        //conf.setLong("KmerCov", kmer_cov);
        // conf.setLong("AllKmer", allkmer);

		BrushConfig.initializeConfiguration(conf);

        
        FileInputFormat.addInputPaths(conf, inputPath);
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(TagTrustedReads.TagTrustedReadsMapper.class);
		conf.setReducerClass(TagTrustedReads.TagTrustedReadsReducer.class);

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

		run(inputPath, outputPath);

		long endtime = System.currentTimeMillis();

		float diff = (float) (((float) (endtime - starttime)) / 1000.0);

		System.out.println("Runtime: " + diff + " s");

		return 0;
	}

	public static void main(String[] args) throws Exception
	{
		int res = ToolRunner.run(new Configuration(), new TagTrustedReads(), args);
		System.exit(res);
	}
}


