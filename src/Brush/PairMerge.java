/*
    PairMerge.java
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


public class PairMerge extends Configured implements Tool 
{	
	private static final Logger sLogger = Logger.getLogger(PairMerge.class);
	
	private static class PairMergeMapper extends MapReduceBase 
	implements Mapper<LongWritable, Text, Text, Text> 
	{
		public void map(LongWritable lineid, Text nodetxt,
				OutputCollector<Text, Text> output, Reporter reporter)
		throws IOException 
		{
			Node node = new Node();
			node.fromNodeMsg(nodetxt.toString());

			String mergedir = node.getMerge();

			if (mergedir != null)
			{
				TailInfo compressed = node.gettail(mergedir);

				output.collect(new Text(compressed.id),
						new Text(Node.COMPRESSPAIR + "\t" + mergedir + "\t" + compressed.dir + "\t" + compressed.oval_size + "\t" + node.toNodeMsg(true)));
				
				reporter.incrCounter("Brush", "mergenodes", 1);
			}
			else
			{
				output.collect(new Text(node.getNodeId()), new Text(node.toNodeMsg()));
			}

			reporter.incrCounter("Brush", "nodes", 1);
		}
	}
	
	private static class PairMergeReducer extends MapReduceBase 
	implements Reducer<Text, Text, Text, Text> 
	{
		private static int K = 0;
		private static boolean VERBOSE = false;
		
		public void configure(JobConf job) {
			K = Integer.parseInt(job.get("K"));
		}
		
		class CompressInfo
		{
			String dir;
			String bdir;
            String oval_size;
			Node   node;
			
			public String toString()
			{
				return node.getNodeId() + " " + dir + " " + bdir;
			}
		}
		
		public void mergepair(Node node, CompressInfo ci) throws IOException
		{
		      if (VERBOSE)
		      {
		        System.err.println("[==");
		        System.err.println("Merging " + node.getNodeId() + " " + ci);
		        System.err.println(node.toNodeMsg(true));
		        System.err.println(ci.node.toNodeMsg(true));
		      }

		      // update the node string
		      String astr = ci.node.str();
		      if (ci.dir.equals("r")) 
		      { 
		        astr = Node.rc(astr);
		        //ci.node.revreads();
		      }

		      String bstr = node.str();
		      if (ci.bdir.equals("r")) 
		      { 
		        bstr = Node.rc(bstr); 
		        //node.revreads();
		      }

              int Oval_Size = Integer.parseInt(ci.oval_size);
		      //int shift = astr.length() - K + 1;
		    //  ci.node.addreads(node, shift);
		    //  node.setreads(ci.node);

		      String str = Node.str_concat(astr, bstr, Oval_Size);
		      if (ci.bdir.equals("r")) { str = Node.rc(str); }
		      
		      node.setstr(str);

		      /*if (ci.bdir.equals("r"))
		      {
		    	  node.revreads();
		      }*/

		      int amerlen = astr.length() - Oval_Size + 1;
		      int bmerlen = bstr.length() - Oval_Size + 1;
		      
		      float ncov = node.cov();
		      float ccov = ci.node.cov();
		      
		      //node.setCoverage(((ncov * amerlen) + (ccov * bmerlen)) / (amerlen + bmerlen));
              node.setCoverage( (ncov*astr.length() + ccov * bstr.length()) / (astr.length()+bstr.length()-Oval_Size) );

              node.addPairEnd(ci.node.getNodeId());
              if (ci.node.getPairEnds() != null){
                node.addAllPairEnd(ci.node.getPairEnds());
              }
             
		      /*List<String> threads = ci.node.getThreads();
		      if (threads != null)
		      {
		    	  if (!ci.bdir.equals(ci.dir))
		    	  {
		    		  // Flip the direction of the threads
		    		  for(String thread : threads)
		    		  {
		    			  String [] vals = thread.split(":"); // t link r

		    			  String ta = Node.flip_dir(vals[0].substring(0,1));
		    			  String tb = vals[0].substring(1,2);

		    			  node.addThread(ta+tb, vals[1], vals[2]);
		    		  }
		    	  }
		    	  else
		    	  {
		    		  for(String thread : threads)
		    		  {
		    			  node.addThread(thread);
		    		  }
		    	  }
		      }*/

		      // update the appropriate neighbors with $cnode's pointers

		      //print " orig: $cdir $cbdir\n";
		      
		      ci.dir = Node.flip_dir(ci.dir);
		      ci.bdir = Node.flip_dir(ci.bdir);

		      for(String adj : Node.dirs)
		      {
		        String key = ci.dir + adj;
		        String fkey = ci.bdir + adj;

		        //print "  Updating my $fkey with cnode $key\n";
		        
		        List<String> ce = ci.node.getEdges(key);
		        node.setEdges(fkey, ce);
		      }

		      // Now update the can compress flag
              //if (node.getBlackEdges() == null) { // for path compress
                node.setCanCompress(ci.bdir, ci.node.canCompress(ci.dir));
              //}

		      if (VERBOSE) { System.err.println(node.toNodeMsg());}
		}
		
		public void reduce(Text nodeid, Iterator<Text> iter,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException 
		{
			Node node = new Node(nodeid.toString());
			
			int sawnode = 0;
			
			CompressInfo fci = null;
			CompressInfo rci = null;
			
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
					CompressInfo ci = new CompressInfo();
					
					ci.dir  = vals[1];
					ci.bdir = vals[2];
                    ci.oval_size = vals[3];
					ci.node = new Node(vals[4]);
					ci.node.parseNodeMsg(vals, 5);
					
					if (ci.bdir.equals("f"))
					{
						if (fci != null)
						{
							throw new IOException("Multiple f compresses to " + nodeid.toString());
						}
						
						fci = ci;
					}
					else
					{
						if (rci != null)
						{
							throw new IOException("Multiple r compresses to " + nodeid.toString());
						}
						
						rci = ci;
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

			if (fci != null) { mergepair(node, fci); }
			if (rci != null) { mergepair(node, rci); }

			//node.cleanThreads();

			// Update the tail pointers, and cancompress flag
			for (String adj : Node.dirs)
			{
				TailInfo ti = node.gettail(adj);

				// check for a cycle, and break link
				if ((ti == null) || ti.id.equals(node.getNodeId()))
				{
					node.setCanCompress(adj, false);
				}
			}

			if (node.canCompress("f") || node.canCompress("r"))
			{
				reporter.incrCounter("Brush", "needscompress", 1);
			}

			output.collect(nodeid, new Text(node.toNodeMsg()));
		}
	}
	
	
	
	public RunningJob run(String inputPath, String outputPath) throws Exception
	{ 
		sLogger.info("Tool name: PairMerge");
		sLogger.info(" - input: "  + inputPath);
		sLogger.info(" - output: " + outputPath);
		
		JobConf conf = new JobConf(Stats.class);
		conf.setJobName("PairMerge " + inputPath);
		
		BrushConfig.initializeConfiguration(conf);
			
		FileInputFormat.addInputPath(conf, new Path(inputPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(PairMergeMapper.class);
		conf.setReducerClass(PairMergeReducer.class);

		//delete the output directory if it exists already
		FileSystem.get(conf).delete(new Path(outputPath), true);

		return JobClient.runJob(conf);
	}
	
	
	public int run(String[] args) throws Exception 
	{
		String inputPath  = "";
		String outputPath = "";
		BrushConfig.K = 21;
		run(inputPath, outputPath);
		
		return 0;
	}

	public static void main(String[] args) throws Exception 
	{
		int res = ToolRunner.run(new Configuration(), new PairMerge(), args);
		System.exit(res);
	}
}
