/*
    QuickMark.java
    Author: Chien-Chih Chen (rocky@iis.sinica.edu.tw)

    This file is derived from Contrail Project on SourceForge, 
    released under Apache License 2.0 (http://www.apache.org/licenses/LICENSE-2.0), 
    developed by Michael Schatz, Jeremy Chambers, Avijit Gupta, Rushil Gupta, 
    David Kelley, Jeremy Lewi, Deepak Nettem, Dan Sommer, and Mihai Pop, 
    Schatz Lab and Cold Spring Harbor Laboratory.

    More information about Contrail Project you can find under: 
    http://sourceforge.net/apps/mediawiki/contrail-bio/
*/


package Brush;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
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

public class QuickMark extends Configured implements Tool
{
	private static final Logger sLogger = Logger.getLogger(QuickMark.class);

	public static class QuickMarkMapper extends MapReduceBase
    implements Mapper<LongWritable, Text, Text, Text>
	{
		public void map(LongWritable lineid, Text nodetxt,
                OutputCollector<Text, Text> output, Reporter reporter)
                throws IOException
        {
			Node node = new Node();
			node.fromNodeMsg(nodetxt.toString());

			if (node.canCompress("f") || node.canCompress("r"))
			{
				// tell all of my neighbors I intend to compress
				reporter.incrCounter("Brush", "compressible", 1);

				for(String et : Node.edgetypes)
				{
					List<String> edges = node.getEdges(et);
					if (edges != null)
					{
						for (String v : edges)
						{
                            //\\// modify for overlap graph
                            int idx = v.indexOf("!");
                            if (idx == -1) {
                                throw new IOException( node.getNodeId() + "'s edge:" + v + " without overlap size information!! ");
                            } else {
                                output.collect(new Text(v.substring(0, idx)), new Text(Node.COMPRESSPAIR));
                            }
						}
					}
				}
			}

			output.collect(new Text(node.getNodeId()), new Text(node.toNodeMsg()));

			reporter.incrCounter("Brush", "nodes", 1);
        }
	}

	public static class QuickMarkReducer extends MapReduceBase
	implements Reducer<Text, Text, Text, Text>
	{
		public void reduce(Text nodeid, Iterator<Text> iter,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException
		{
			boolean compresspair = false;

			Node node = new Node(nodeid.toString());

			int sawnode = 0;

			while(iter.hasNext())
			{
				String msg = iter.next().toString();

				//System.err.println(key.toString() + "\t" + msg);

				String [] vals = msg.split("\t");

				if (vals[0].equals(Node.NODEMSG))
				{
					node.parseNodeMsg(vals, 0);
					sawnode++;
				}
				else if (vals[0].equals(Node.COMPRESSPAIR))
				{
					compresspair = true;
				}
				else
				{
					throw new IOException("Unknown msgtype: " + msg);
				}
			}

			if (sawnode != 1)
			{
				throw new IOException("ERROR: Didn't see exactly 1 nodemsg (" + sawnode + ") for " + nodeid.toString());
			}

			if (node.canCompress("f") || node.canCompress("r") || compresspair)
			{
				node.setMertag("0");
				reporter.incrCounter("Brush", "compressibleneighborhood", 1);
			}
			else
			{
				node.setMertag(Integer.toHexString(node.getNodeId().hashCode()));
			}

			output.collect(nodeid, new Text(node.toNodeMsg()));
		}
	}


	public RunningJob run(String inputPath, String outputPath) throws Exception
	{
		sLogger.info("Tool name: QuickMark");
		sLogger.info(" - input: "  + inputPath);
		sLogger.info(" - output: " + outputPath);

		JobConf conf = new JobConf(QuickMark.class);
        //JobConf conf = new JobConf(Stats.class);
		conf.setJobName("QuickMark " + inputPath);

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

		conf.setMapperClass(QuickMarkMapper.class);
		conf.setReducerClass(QuickMarkReducer.class);

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
		int res = ToolRunner.run(new Configuration(), new QuickMark(), args);
		System.exit(res);
	}
}
