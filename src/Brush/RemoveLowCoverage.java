/*
    RemoveLowCoverage.java
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
import java.util.ArrayList;
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


public class RemoveLowCoverage extends Configured implements Tool
{
	private static final Logger sLogger = Logger.getLogger(RemoveLowCoverage.class);


	// RemoveLowCoverageMapper
	///////////////////////////////////////////////////////////////////////////

	private static class RemoveLowCoverageMapper extends MapReduceBase
    implements Mapper<LongWritable, Text, Text, Text>
	{
		private static int K = 0;
		public static float MAX_LOW_COV_LEN = 0;
		public static float LOW_COV_THRESH = 1.0f;

		public void configure(JobConf job)
		{
			K = Integer.parseInt(job.get("K"));
			//MAX_LOW_COV_LEN = Long.parseLong(job.get("MAX_LOW_COV_LEN"));
			LOW_COV_THRESH = Float.parseFloat(job.get("LOW_COV_THRESH"));
            //LOW_COV_THRESH = 1f;
            MAX_LOW_COV_LEN = (LOW_COV_THRESH+1)*Long.parseLong(job.get("READLENGTH")) - LOW_COV_THRESH*K;
		}

		public void map(LongWritable lineid, Text nodetxt,
                OutputCollector<Text, Text> output, Reporter reporter)
		throws IOException
		{
			Node node = new Node();
			node.fromNodeMsg(nodetxt.toString());

			reporter.incrCounter("Brush", "nodes", 1);

			int len = node.len();
			float cov = node.cov();
            int fdegree = node.degree("f");
			int rdegree = node.degree("r");
          

			if ((len <= MAX_LOW_COV_LEN) && (cov <= LOW_COV_THRESH) )
			{
				//System.err.println("Deleting low coverage node " + node.getNodeId() + " len=" + len + " cov=" + cov);
				reporter.incrCounter("Brush", "lowcovremoved", 1);

				int degree = 0;

				for(String et : Node.edgetypes)
				{
					List<String> edges = node.getEdges(et);

					if (edges != null)
					{
						String ret = Node.flip_link(et);

						for(String v : edges)
						{
							String p = v.substring(0, v.indexOf("!"));
							degree++;
							output.collect(new Text(p), new Text(Node.TRIMMSG + "\t" + ret + "\t" + node.getNodeId()));
						}
					}
				}

				if (degree == 0)
				{
					reporter.incrCounter("Brush", "lowcoverage_island", 1);
				}
			}
			else
			{
				output.collect(new Text(node.getNodeId()), new Text(node.toNodeMsg()));
			}
            //output.collect(new Text(node.getNodeId()), new Text(node.toNodeMsg()));
		}
	}


	// RemoveLowCoverageReducer
	///////////////////////////////////////////////////////////////////////////

	private static class RemoveLowCoverageReducer extends MapReduceBase
	implements Reducer<Text, Text, Text, Text>
	{
		private static int K = 0;

		public class EdgeInfo
		{
			String id;
			String et;

			public EdgeInfo(String pet, String pid)
			{
				et = pet;
				id = pid;
			}
		}

		public void configure(JobConf job) {
			K = Integer.parseInt(job.get("K"));
		}

		public void reduce(Text nodeid, Iterator<Text> iter,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException
		{
			Node node = new Node(nodeid.toString());
			List<EdgeInfo> edges = new ArrayList<EdgeInfo>();

			int sawnode = 0;

			while(iter.hasNext())
			{
				String msg = iter.next().toString();

				//System.err.println(nodeid.toString() + "\t" + msg);

				String [] vals = msg.split("\t");

				if (vals[0].equals(Node.NODEMSG))
				{
					node.parseNodeMsg(vals, 0);
					sawnode++;
				}
				else if (vals[0].equals(Node.TRIMMSG))
				{
					EdgeInfo edge = new EdgeInfo(vals[1], vals[2]);
					edges.add(edge);
				}
				else
				{
					throw new IOException("Unknown msgtype: " + msg);
				}
			}

			// there could be adjacent low coverage nodes
			if (sawnode > 1)
			{
                throw new IOException("ERROR: Didn't see exactly 1 nodemsg (" + sawnode + ") for " + nodeid.toString());
			}
			else if (sawnode == 1)
			{
				if (edges.size() > 0)
				{
					for(EdgeInfo edge : edges)
					{
						node.removelink(edge.id, edge.et);
						reporter.incrCounter("Brush", "linksremoved", 1);
					}
				}
				output.collect(nodeid, new Text(node.toNodeMsg()));
			}
		}
	}




	// Run Tool
	///////////////////////////////////////////////////////////////////////////

	public RunningJob run(String inputPath, String outputPath) throws Exception
	{
		sLogger.info("Tool name: RemoveLowCoverage");
		sLogger.info(" - input: "  + inputPath);
		sLogger.info(" - output: " + outputPath);

		JobConf conf = new JobConf(RemoveLowCoverage.class);
		conf.setJobName("RemoveLowCoverage " + inputPath + " " + BrushConfig.K);

		BrushConfig.initializeConfiguration(conf);

		FileInputFormat.addInputPath(conf, new Path(inputPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(RemoveLowCoverageMapper.class);
		conf.setReducerClass(RemoveLowCoverageReducer.class);

		//delete the output directory if it exists already
		FileSystem.get(conf).delete(new Path(outputPath), true);

		return JobClient.runJob(conf);
	}


	// Parse Arguments and run
	///////////////////////////////////////////////////////////////////////////

	public int run(String[] args) throws Exception
	{
		String inputPath  = "";
		String outputPath = "";

		BrushConfig.K = 21;
		BrushConfig.MAX_LOW_COV_LEN = 42;
		BrushConfig.LOW_COV_THRESH = 5.0f;

		run(inputPath, outputPath);
		return 0;
	}


	// Main
	///////////////////////////////////////////////////////////////////////////

	public static void main(String[] args) throws Exception
	{
		int res = ToolRunner.run(new Configuration(), new RemoveLowCoverage(), args);
		System.exit(res);
	}
}
