/*
    PairMark.java
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
import java.util.Iterator;
import java.util.List;
import java.util.Random;

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


public class PairMark extends Configured implements Tool 
{	
	private static final Logger sLogger = Logger.getLogger(PairMark.class);
	
	private static class PairMarkMapper extends MapReduceBase 
    implements Mapper<LongWritable, Text, Text, Text> 
	{
		private static long randseed = 0;
		private Random rfactory = new Random();
		
		public void configure(JobConf job) 
		{
			randseed = Long.parseLong(job.get("randseed"));
		}
		
		public boolean isMale(String nodeid)
		{
			rfactory.setSeed(nodeid.hashCode() ^ randseed);
			
			double rand = rfactory.nextDouble();
			
			boolean male = (rand >= .5);
			
			//System.err.println(nodeid + " " + rand + " " + male);

			return male;
		}
		
		public TailInfo getBuddy(Node node, String dir)
		{
			if (node.canCompress(dir))
			{
				return node.gettail(dir);
			}
			
			return null;
		}
		
		public void map(LongWritable lineid, Text nodetxt,
                OutputCollector<Text, Text> output, Reporter reporter)
                throws IOException 
        {
			Node node = new Node();
			node.fromNodeMsg(nodetxt.toString());

			TailInfo fbuddy = getBuddy(node, "f");
			TailInfo rbuddy = getBuddy(node, "r");

			if (((fbuddy != null) || (rbuddy != null)) /*&& (node.getBlackEdges() == null)*//*for path compress*/)
			{
				String nodeid = node.getNodeId();
				
				reporter.incrCounter("Brush", "compressible", 1);

				String compress = null;
				String compressdir = null;
				String compressbdir = null;

				if (isMale(node.getNodeId()))
				{
					// Prefer Merging forward
					if (fbuddy != null)
					{
						boolean fmale = isMale(fbuddy.id);

						if (!fmale)
						{
							compress     = fbuddy.id;
							compressdir  = "f";
							compressbdir = fbuddy.dir;
						}
					}

					if ((compress == null) && (rbuddy != null))
					{
						boolean rmale = isMale(rbuddy.id);

						if (!rmale)
						{
							compress     = rbuddy.id;
							compressdir  = "r";
							compressbdir = rbuddy.dir;
						}
					}
				}
				else
				{
					if ((rbuddy != null) && (fbuddy != null))
					{
						boolean fmale = isMale(fbuddy.id);
						boolean rmale = isMale(rbuddy.id);

						if (!fmale && !rmale &&
								(nodeid.compareTo(fbuddy.id) < 0) && 
								(nodeid.compareTo(rbuddy.id) < 0))
						{
							// FFF and I'm the local minimum, go ahead and compress
							compress     = fbuddy.id;
							compressdir  = "f";
							compressbdir = fbuddy.dir;
						}
					}
					else if (rbuddy == null)
					{
						boolean fmale = isMale(fbuddy.id);

						if (!fmale && (nodeid.compareTo(fbuddy.id) < 0))
						{
							// Its X*=>FF and I'm the local minimum
							compress     = fbuddy.id;
							compressdir  = "f";
							compressbdir = fbuddy.dir;
						}
					}
					else if (fbuddy == null)
					{
						boolean rmale = isMale(rbuddy.id);

						if (!rmale && (nodeid.compareTo(rbuddy.id) < 0))
						{
							// Its FF=>X* and I'm the local minimum
							compress     = rbuddy.id;
							compressdir  = "r";
							compressbdir = rbuddy.dir;
						}
					}
				}

				if (compress != null)
				{
					//print STDERR "compress $nodeid $compress $compressdir $compressbdir\n";
					reporter.incrCounter("Brush","mergestomake", 1);

					//Save that I'm supposed to merge
					node.setMerge(compressdir);

					// Now tell my ~CD neighbors about my new nodeid
					String toupdate = Node.flip_dir(compressdir);

					for(String adj : Node.dirs)
					{
						String key = toupdate + adj;

						String origadj = Node.flip_dir(adj) + compressdir;
						String newadj  = Node.flip_dir(adj) + compressbdir;

						List<String> edges = node.getEdges(key);

						if (edges != null)
						{
							for (String p : edges)
							{
								reporter.incrCounter("Brush", "remoteupdate", 1);
                                String edge_id = p.substring(0, p.indexOf("!"));
                                String oval_size = p.substring(p.indexOf("!")+1);
								output.collect(new Text(edge_id),
										       new Text(Node.UPDATEMSG + "\t" + nodeid + "\t" + origadj + "\t" + compress + "\t" + newadj + "\t" + oval_size));
							}
						}
					}
				}
			}

			output.collect(new Text(node.getNodeId()), new Text(node.toNodeMsg()));
			reporter.incrCounter("Brush", "nodes", 1);
        }
	}
	
	private static class PairMarkReducer extends MapReduceBase 
	implements Reducer<Text, Text, Text, Text> 
	{
		private static long randseed = 0;
		
		public void configure(JobConf job) {
			randseed = Long.parseLong(job.get("randseed"));
		}
		
		private class Update
		{
			public String oid;
			public String odir;
			public String nid;
			public String ndir;
            public String oval_size;
		}
		
		public void reduce(Text nodeid, Iterator<Text> iter,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException 
		{
			Node node = new Node(nodeid.toString());
			List<Update> updates = new ArrayList<Update>();
			
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
				else if (vals[0].equals(Node.UPDATEMSG))
				{
					Update up = new Update();
					
					up.oid  = vals[1];
					up.odir = vals[2];
					up.nid  = vals[3];
					up.ndir = vals[4];
                    up.oval_size = vals[5];
					
					updates.add(up);
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
			
			if (updates.size() > 0)
			{
				for(Update up : updates)
				{
					node.replacelink(up.oid + "!" + up.oval_size, up.odir, up.nid + "!" + up.oval_size, up.ndir);
				}
			}
			
			output.collect(nodeid, new Text(node.toNodeMsg()));
		}
	}

	
	public RunningJob run(String inputPath, String outputPath, long randseed) throws Exception
	{ 
		sLogger.info("Tool name: PairMark");
		sLogger.info(" - input: "  + inputPath);
		sLogger.info(" - output: " + outputPath);
		sLogger.info(" - randseed: " + randseed);
		
		JobConf conf = new JobConf(PairMark.class);
		conf.setJobName("PairMark " + inputPath);
		
		BrushConfig.initializeConfiguration(conf);
		conf.setLong("randseed", randseed);
			
		FileInputFormat.addInputPath(conf, new Path(inputPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(PairMarkMapper.class);
		conf.setReducerClass(PairMarkReducer.class);

		//delete the output directory if it exists already
		FileSystem.get(conf).delete(new Path(outputPath), true);

		return JobClient.runJob(conf);
	}
	
	
	public int run(String[] args) throws Exception 
	{
		String inputPath  = "";
		String outputPath = "";
		long randseed = 123456789;
		
		run(inputPath, outputPath, randseed);
		
		return 0;
	}

	public static void main(String[] args) throws Exception 
	{
		int res = ToolRunner.run(new Configuration(), new PairMark(), args);
		System.exit(res);
	}
}
