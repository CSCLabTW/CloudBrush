/*
    EdgeRemoval.java
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



public class EdgeRemoval extends Configured implements Tool
{
	private static final Logger sLogger = Logger.getLogger(PopBubbles.class);


	// PopBubblesMapper
	///////////////////////////////////////////////////////////////////////////

	public static class EdgeRemovalMapper extends MapReduceBase
    implements Mapper<LongWritable, Text, Text, Text>
	{
		private static int K = 0;

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

			List<String> r_edges = node.getRemovalEdges();
			if (r_edges != null)
			{
				for(String r_edge : r_edges)
				{
					String [] vals = r_edge.split("\\|");
					String id    = vals[0];
					String dir   = vals[1];
					String dead     = vals[2];
                    int oval = Integer.parseInt(vals[3]);

					output.collect(new Text(id),
							       new Text(Node.KILLLINKMSG + "\t" + dir + "\t" + dead+ "\t" + oval));

					reporter.incrCounter("Brush", "edgesremoved", 1);
				}

				node.clearRemovalEdge();
			}

			output.collect(new Text(node.getNodeId()), new Text(node.toNodeMsg()));
			reporter.incrCounter("Brush", "nodes", 1);
		}
	}

	// EdgeRemovalReducer
	///////////////////////////////////////////////////////////////////////////

	public static class EdgeRemovalReducer extends MapReduceBase
	implements Reducer<Text, Text, Text, Text>
	{
		private static int K = 0;

		public void configure(JobConf job) {
			K = Integer.parseInt(job.get("K"));
		}

		public class RemoveLink
		{
		    public String deaddir;
		    public String deadid;
            public int oval_size;

		    public RemoveLink(String[] vals, int offset) throws IOException
		    {
		    	if (!vals[offset].equals(Node.KILLLINKMSG))
		    	{
		    		throw new IOException("Unknown msg");
		    	}

		    	deaddir = vals[offset+1];
		    	deadid  = vals[offset+2];
                oval_size = Integer.parseInt(vals[offset+3]);
		    }
		}

		public void reduce(Text nodeid, Iterator<Text> iter,
				OutputCollector<Text, Text> output, Reporter reporter)
		throws IOException
		{
			Node node = new Node(nodeid.toString());

			int sawnode = 0;

			boolean killnode = false;
			float extracov = 0;
			List<RemoveLink> links = new ArrayList<RemoveLink>();

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
				else if (vals[0].equals(Node.KILLLINKMSG))
				{
					RemoveLink link = new RemoveLink(vals, 0);
					links.add(link);
				}
				else
				{
					throw new IOException("Unknown msgtype: " + msg);
				}
			}

			if (sawnode != 1)
			{
				if (node.getEdges("ff") == null && node.getEdges("rr") == null && node.getEdges("fr") == null && node.getEdges("rf") == null){
                    // do nothing, possible black node
                } else {
                    throw new IOException("ERROR: Didn't see exactly 1 nodemsg (" + sawnode + ") for " + nodeid.toString());
                }
			}

			if (links.size() > 0)
			{
				for(RemoveLink link : links)
				{
                    //\\\\\\\\\\\\\\\\\
                   /* List<String> edges = node.getEdges(link.deaddir);
                    if (edges == null) {
                        // do nothing
                    } else {
                        //node.clearEdges(link.dir);
                        for (Iterator it = node.getEdges(link.deaddir).iterator();it.hasNext();){    //reparations�慢ollection
                            String v = (String)it.next();
                            String [] vals = v.split("!");
                            if (vals[0].equals(link.deadid))
                            {
                                if (link.oval_size == Integer.parseInt(vals[1])) {
                                    it.remove();
                                    reporter.incrCounter("Brush", "linksremoved", 1);
                                    break;
                                }
                            }
                        }
                    }*/
                    //\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\
                    if (node.hasEdge(link.deaddir, link.deadid, link.oval_size)){
                        node.removelink(link.deadid, link.deaddir, link.oval_size);
                    }
					reporter.incrCounter("Brush", "linksremoved", 1);
				}


				//int threadsremoved = node.cleanThreads();
				//reporter.incrCounter("Contrail", "threadsremoved", 1);
			}

			output.collect(nodeid, new Text(node.toNodeMsg()));
		}
	}




	// Run Tool
	///////////////////////////////////////////////////////////////////////////

	public RunningJob run(String inputPath, String outputPath) throws Exception
	{
		sLogger.info("Tool name: EdgeRemoval");
		sLogger.info(" - input: "  + inputPath);
		sLogger.info(" - output: " + outputPath);

		JobConf conf = new JobConf(EdgeRemoval.class);
		conf.setJobName("EdgeRemoval " + inputPath + " " + BrushConfig.K);

		BrushConfig.initializeConfiguration(conf);

		FileInputFormat.addInputPath(conf, new Path(inputPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);
        //conf.setBoolean("mapred.output.compress", true);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(EdgeRemovalMapper.class);
		conf.setReducerClass(EdgeRemovalReducer.class);

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

		run(inputPath, outputPath);
		return 0;
	}


	// Main
	///////////////////////////////////////////////////////////////////////////

	public static void main(String[] args) throws Exception
	{
		int res = ToolRunner.run(new Configuration(), new EdgeRemoval(), args);
		System.exit(res);
	}
}


