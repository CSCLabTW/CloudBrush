/*
    TipsRemoval.java
    2012 Ⓒ CloudBrush, developed by Chien-Chih Chen (rocky@iis.sinica.edu.tw), 
    released under Apache License 2.0 (http://www.apache.org/licenses/LICENSE-2.0) 
    at: https://github.com/ice91/CloudBrush

    The file is derived from Contrail Project which is developed by Michael Schatz, 
    Jeremy Chambers, Avijit Gupta, Rushil Gupta, David Kelley, Jeremy Lewi, 
    Deepak Nettem, Dan Sommer, Mihai Pop, Schatz Lab and Cold Spring Harbor Laboratory, 
    and is released under Apache License 2.0 at: 
    http://sourceforge.net/apps/mediawiki/contrail-bio/
*/

package Brush;

import java.io.IOException;
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
/**
 *
 * @author rocky
 */
public class TipsRemoval extends Configured implements Tool
{
    private static final Logger sLogger = Logger.getLogger(TipsRemoval.class);

    // TipsRemovalMapper
	///////////////////////////////////////////////////////////////////////////
    public static class TipsRemovalMapper extends MapReduceBase
    implements Mapper<LongWritable, Text, Text, Text>
    {
        private static int K = 0;
		public static int TIPLENGTH = 0;

        public void configure(JobConf job) 
		{
			K = Integer.parseInt(job.get("K"));
			TIPLENGTH = Integer.parseInt(job.get("TIPLENGTH"));
		}

        public void map(LongWritable lineid, Text nodetxt,
                OutputCollector<Text, Text> output, Reporter reporter)
        throws IOException
		{
            Node node = new Node();
            node.fromNodeMsg(nodetxt.toString());

            int fdegree = node.degree("f");
			int rdegree = node.degree("r");
			float len = node.len() * node.cov();

            
            if ((len <= (float)TIPLENGTH) && (fdegree + rdegree <= 1) /*&& node.isUnique()*/)
			{
				reporter.incrCounter("Brush", "tips_found", 1);

				if ((fdegree == 0) && (rdegree == 0))
				{
					//this node is not connected to the rest of the graph
					//nothing to do
					reporter.incrCounter("Brush", "tips_island", 1);
				}
				else
				{
					// Tell the one neighbor that I'm a tip
					String linkdir = (fdegree == 0) ? "r" : "f";

					for(String adj : Node.dirs)
					{
						String key = linkdir + adj;

						List<String> edges = node.getEdges(key);

						if (edges != null)
						{
							if (edges.size() != 1)
							{
								throw new IOException("Expected a single edge from " + node.getNodeId());
							}

                            String edge_content = edges.get(0);
							String p = edge_content.substring(0, edge_content.indexOf("!"));

							if (p.equals(node.getNodeId()))
							{
								// short tandem repeat, trim away
								reporter.incrCounter("Brush", "tips_shorttandem", 1);
							}
							else
							{
								String con = Node.flip_dir(adj) + Node.flip_dir(linkdir);
								output.collect(new Text(p), new Text(Node.TRIMMSG + "\t" + con + "\t" + nodetxt));
							}
						}
					}
				}
			}
			else
			{
				output.collect(new Text(node.getNodeId()), new Text(node.toNodeMsg()));
			}

            reporter.incrCounter("Brush", "nodes", 1);
        }

    }

    // TipsRemovalReducer
	///////////////////////////////////////////////////////////////////////////
    public static class TipsRemovalReducer extends MapReduceBase
	implements Reducer<Text, Text, Text, Text>
	{
        public void reduce(Text nodeid, Iterator<Text> iter,
				OutputCollector<Text, Text> output, Reporter reporter)
		throws IOException
        {
            Node node = new Node(nodeid.toString());
			Map<String, List<Node>> tips = new HashMap<String, List<Node>>();

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
				else if (vals[0].equals(Node.TRIMMSG))
				{
					String adj = vals[1];

					Node tip = new Node(vals[2]);
					tip.parseNodeMsg(vals, 3);

					if (tips.containsKey(adj))
					{
						tips.get(adj).add(tip);
					}
					else
					{
						List<Node> tiplist = new ArrayList<Node>();
						tiplist.add(tip);
						tips.put(adj, tiplist);
					}
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

			if (tips.size() != 0)
			{
				for(String d : Node.dirs)
				{
					int deg = node.degree(d);

					int numtrim = 0;

					List<Node> ftips = tips.get(d+"f");
					List<Node> rtips = tips.get(d+"r");

					if (ftips != null) { numtrim += ftips.size(); }
					if (rtips != null) { numtrim += rtips.size(); }

					if (numtrim == 0) { continue; }

					Node besttip = null;
					int bestlen = 0;

					if (numtrim == deg)
					{
						// All edges in this direction are tips, only keep the longest one

						if (ftips != null)
						{
							for (Node t : ftips)
							{
                                //\\
                                TailInfo tail = t.gettail("r");
                                //\\
								if ((t.len() - tail.oval_size) >= bestlen)
								{
									bestlen = t.len() - tail.oval_size;
									besttip = t;
								}
							}
						}

						if (rtips != null)
						{
							for (Node t : rtips)
							{
                                //\\
                                TailInfo tail = t.gettail("f");
                                //\\
								if ((t.len() - tail.oval_size) > bestlen)
								{
									bestlen = t.len() - tail.oval_size;
									besttip = t;
								}
							}
						}
                        if (besttip == null) {
                            throw new IOException("Best Tip Error - node_id:" + node.getNodeId() + " num_trim:" + numtrim + " degree:" + deg);
                        }
						output.collect(new Text(besttip.getNodeId()), new Text(besttip.toNodeMsg()));
						reporter.incrCounter("Brush", "tips_kept", 1);
					}


					if (ftips != null)
					{
						String adj = d+"f";

						for (Node t : ftips)
						{
							if (t != besttip)
							{
								node.removelink(t.getNodeId(), adj);
								reporter.incrCounter("Brush", "tips_clipped", 1);
							}
						}
					}

					if (rtips != null)
					{
						String adj = d+"r";

						for (Node t : rtips)
						{
							if (t != besttip)
							{
								node.removelink(t.getNodeId(), adj);
								reporter.incrCounter("Brush", "tips_clipped", 1);
							}
						}
					}
				}
			}

			output.collect(nodeid, new Text(node.toNodeMsg()));
        }

    }

    // Run
	///////////////////////////////////////////////////////////////////////////

	public RunningJob run(String inputPath, String outputPath) throws Exception
	{
		sLogger.info("Tool name: TipsRemoval");
		sLogger.info(" - input: "  + inputPath);
		sLogger.info(" - output: " + outputPath);

		JobConf conf = new JobConf(TipsRemoval.class);
		conf.setJobName("RemoveTips " + inputPath + " " + BrushConfig.K);
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

		conf.setMapperClass(TipsRemovalMapper.class);
		conf.setReducerClass(TipsRemovalReducer.class);

		//delete the output directory if it exists already
		FileSystem.get(conf).delete(new Path(outputPath), true);

		return JobClient.runJob(conf);
	}

    // Parse Arguments and Run
	///////////////////////////////////////////////////////////////////////////

	public int run(String[] args) throws Exception
	{
		String inputPath  = "";
		String outputPath = "";
		run(inputPath, outputPath);
        return 0;
	}

    // Main
	///////////////////////////////////////////////////////////////////////////

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new TipsRemoval(), args);
        System.exit(res);
    }
}
