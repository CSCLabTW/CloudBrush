/*
    RedundantRemoval.java
    2012 Ⓒ CloudBrush, developed by Chien-Chih Chen (rocky@iis.sinica.edu.tw), 
    released under Apache License 2.0 (http://www.apache.org/licenses/LICENSE-2.0) 
    at: https://github.com/ice91/CloudBrush
*/

package Brush;
import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
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

public class RedundantRemoval extends Configured implements Tool
{
	private static final Logger sLogger = Logger.getLogger(RedundantRemoval.class);

	public static class RedundantRemovalMapper extends MapReduceBase
    implements Mapper<LongWritable, Text, Text, Text>
	{
		private static Node node = new Node();

        public void map(LongWritable lineid, Text nodetxt,
                OutputCollector<Text, Text> output, Reporter reporter)
                throws IOException
        {
			node.fromNodeMsg(nodetxt.toString());
            output.collect(new Text(node.getNodeId()), new Text(node.toNodeMsg()));
        }
	}

	public static class RedundantRemovalReducer extends MapReduceBase
	implements Reducer<Text, Text, Text, Text>
	{
		public void reduce(Text key, Iterator<Text> iter,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException
		{
            Node node = new Node(key.toString());
            boolean contained = false;

			int sawnode = 0;

			while(iter.hasNext())
			{
				String msg = iter.next().toString();

				//System.err.println(key.toString() + "\t" + msg);

				String [] vals = msg.split("\t");

				if (vals[0].equals(Node.NODEMSG))
				{
					node.parseNodeMsg(vals, 0);
                    if (node.hasCustom("n")) {
                        contained = true;
                    }
					sawnode++;
				}
				else
				{
					throw new IOException("Unknown msgtype: " + msg);
				}
			}
            if (sawnode != 1)
			{			
                throw new IOException("ERROR: Didn't see exactly 1 nodemsg (" + sawnode + ") for " + key.toString());
			}
            if (!contained) {
                output.collect(new Text(node.getNodeId()), new Text(node.toNodeMsg()));
                reporter.incrCounter("Brush", "nodes", 1);
            } else {
                reporter.incrCounter("Brush", "redundant", 1);
            }		
		}
	}


	public RunningJob run(String inputPath, String outputPath) throws Exception
	{
		sLogger.info("Tool name: RedundantRemoval");
		sLogger.info(" - input: "  + inputPath);
		sLogger.info(" - output: " + outputPath);

		JobConf conf = new JobConf(RedundantRemoval.class);
		conf.setJobName("RedundantRemoval " + inputPath);

		BrushConfig.initializeConfiguration(conf);

		FileInputFormat.addInputPath(conf, new Path(inputPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
        
        //conf.setBoolean("mapred.output.compress", true);

		conf.setMapperClass(RedundantRemovalMapper.class);
		conf.setReducerClass(RedundantRemovalReducer.class);

		//delete the output directory if it exists already
		FileSystem.get(conf).delete(new Path(outputPath), true);

		return JobClient.runJob(conf);
	}


	public int run(String[] args) throws Exception
	{
		String inputPath  = "";
		String outputPath = "";

		run(inputPath, outputPath);

		return 0;
	}

	public static void main(String[] args) throws Exception
	{
		int res = ToolRunner.run(new Configuration(), new RedundantRemoval(), args);
		System.exit(res);
	}
}
