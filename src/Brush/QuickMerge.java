/*
    QuickMerge.java
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;
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


public class QuickMerge extends Configured implements Tool
{
	private static final Logger sLogger = Logger.getLogger(QuickMerge.class);

	private static class QuickMergeMapper extends MapReduceBase
    implements Mapper<LongWritable, Text, Text, Text>
	{
		private static Node node = new Node();

		public void map(LongWritable lineid, Text nodetxt,
                OutputCollector<Text, Text> output, Reporter reporter)
                throws IOException
        {
			node.fromNodeMsg(nodetxt.toString());

			String mertag = node.getMertag();
			output.collect(new Text(mertag), new Text(node.toNodeMsg(true)));
			reporter.incrCounter("Brush", "nodes", 1);
        }
	}

	private static class QuickMergeReducer extends MapReduceBase
	implements Reducer<Text, Text, Text, Text>
	{
		private static int K = 21;
		public static boolean VERBOSE = false;

		public void configure(JobConf job) {
			K = Integer.parseInt(job.get("K"));
		}

		public void reduce(Text mertag, Iterator<Text> iter,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException
		{
		    int saved    = 0;
		    int chains   = 0;
		    int cchains  = 0;
		    int totallen = 0;

		    String DONE = "DONE";

		    // Load the nodes with the same key into memory
			Map<String, Node> nodes = new HashMap<String, Node>();

			//VERBOSE = mertag.toString().equals("32_15326_10836_0_2") || mertag.toString().equals("22_5837_4190_0_2") || mertag.toString().equals("101_8467_7940_0_2");

			while(iter.hasNext())
			{
				Node node = new Node();
				String nodestr = iter.next().toString();

				if (VERBOSE)
				{
					//System.err.println(mertag + " " + nodestr);
				}

				if ((nodes.size() > 10) && (nodes.size() % 10 == 0))
				{
					//System.err.println("Common mer: " + mertag.toString() + " cnt:" + nodes.size());
				}

				node.fromNodeMsg(nodestr);
				nodes.put(node.getNodeId(), node);
			}

			//if (nodes.size() > 10) { System.err.println("Loaded all nodes: " + nodes.size() + "\n"); }

			// Now try to merge each node

			int donecnt = 0;
			for (String nodeid : nodes.keySet())
			{
				donecnt++;
				//if ((donecnt % 10) == 0) { System.err.println("Completed Merges: " + donecnt + "\n"); }

				Node node = nodes.get(nodeid);

				if (node.hasCustom(DONE)) { continue; }
				node.setCustom(DONE, "1");

			 	// {r1,r2} >> rtail -> c1 -> c2 -> c3 -> node -> c4 -> c5 -> c6 -> ftail >> {f1,f2}

				TailInfo rtail = TailInfo.find_tail(nodes, node, "r");
				Node rtnode = nodes.get(rtail.id);

				// catch cycles by looking for the ftail from rtail, not node
				TailInfo ftail = TailInfo.find_tail(nodes, rtnode, rtail.dir);
				Node ftnode = nodes.get(ftail.id);

			    rtnode.setCustom(DONE, "1");
			    ftnode.setCustom(DONE, "1");

			    int chainlen = 1 + ftail.dist;

				chains++;
				totallen += chainlen;

				//VERBOSE = rtail.id.equals("HRJMRHHETMRJHSF") || rtail.id.equals("EECOECEOEECOECA") || rtail.id.equals("ECOECEOEECOECEK");
				if (VERBOSE) { System.err.print(nodeid + " " + chainlen + " " + rtail + " " + ftail + " " + mertag.toString()); }

				int domerge = 0;
				if (chainlen > 1)
				{
					boolean allinmemory = true;

					for(String et : Node.edgetypes)
					{
						List<String> e = rtnode.getEdges(et);
						if (e != null)
						{
							for(String v : e)
							{
                                //\\//
                                int idx = v.indexOf("!");
                                if (idx == -1) {
                                    //System.err.println(rtnode.toNodeMsg());
                                    throw new IOException("Edge without overlap size information!! ");
                                }
								if (!nodes.containsKey(v.substring(0, idx)))
								{
									allinmemory = false;
									break;
								}
							}
						}
					}

					if (allinmemory)       { domerge = 2; }
					else if (chainlen > 2) { domerge = 1; }
				}

				if (VERBOSE) { System.err.println(" domerge=" + domerge); }

			    if (domerge > 0)
			    {
			      chainlen--; // Replace the chain with 1 ftail
			      if (domerge == 1) { chainlen--; } // Need rtail too

			      // start at the rtail, and merge until the ftail

			      if (VERBOSE)
			      {
			        System.err.println("[==");
			        System.err.println(rtnode.toNodeMsg(true));
			      }

			      // mergedir is the direction to merge relative to rtail
			      String mergedir = rtail.dir;

			      TailInfo first = rtnode.gettail(mergedir);
			      Node firstnode = nodes.get(first.id);

			      // quick sanity check
			      TailInfo firsttail = firstnode.gettail(Node.flip_dir(first.dir));
			      if (!rtail.id.equals(firsttail.id))
			      {
			    	  throw new IOException("Rtail->tail->tail != Rtail");
			      }

			      // merge string
			      String mstr = rtnode.str();
                  String end = null;
			      if (mergedir.equals("r"))
			      {
			    	  mstr = Node.rc(mstr);
			    	  //rtnode.revreads();
			      }

			      TailInfo cur = new TailInfo(first);

			      int mergelen = 0;

			      Node curnode = nodes.get(cur.id);

                  //\\// obtain overlap size
                  int Oval_Size = cur.oval_size;
                  //\\//
			      //int merlen = mstr.length() - Oval_Size + 1;
			      int merlen = mstr.length();
                  int covlen = merlen;
                  
                  List<String> pairends = new ArrayList<String>();
			      //double covsum = rtnode.cov() * merlen;
                  double covsum = rtnode.cov() * mstr.length();
			     // int shift = merlen;

			      String lastid = cur.id;
			      String lastdir = cur.dir;

			      while (!cur.id.equals(ftail.id))
			      {
			        curnode = nodes.get(cur.id);

			        if (VERBOSE) { System.err.println(curnode.toNodeMsg(true)); }

			        // curnode can be deleted
			        curnode.setCustom(DONE, "2");
			        mergelen++;

			        String bstr = curnode.str();
			        if (cur.dir.equals("r"))
			        {
			        	bstr = Node.rc(bstr);
			        	//curnode.revreads();
			        }

                    //System.err.println("1 Oval_Size:" + Oval_Size + " id:" + cur.toString());
			        mstr = Node.str_concat(mstr, bstr, Oval_Size);

			        //merlen = bstr.length() - Oval_Size + 1;
                    merlen = bstr.length() - Oval_Size;
			        //covsum += curnode.cov() * merlen;
                    covsum += curnode.cov() * bstr.length();
			        covlen += merlen;

                    pairends.add(curnode.getNodeId());
                    if (curnode.getPairEnds() != null){
                        pairends.addAll(curnode.getPairEnds());
                    }
                    //rtnode.addreads(curnode, shift);
			        //shift += merlen;

			        lastid = cur.id;
			        lastdir = cur.dir;

			        cur = curnode.gettail(lastdir);
                    //\\// obtain overlap size for next cur node
                    Oval_Size = cur.oval_size;
                    
			      }

			      if (VERBOSE) { System.err.println(ftnode.toNodeMsg(true)); }
			      if (VERBOSE) { System.err.println("=="); }

			      // If we made it all the way to the ftail,
			      // see if we should do the final merge
			      if ((domerge == 2) &&
			    		  (cur.id.equals(ftail.id)) &&
			    		  (mergelen == (chainlen-1)))
			      {
			    	  mergelen++;
			    	  rtnode.setCustom(DONE, "2");

			    	  String bstr = ftnode.str();
			    	  if (cur.dir.equals("r"))
			    	  {
			    		  bstr = Node.rc(bstr);
			    		  //ftnode.revreads();
			    	  }
                      //\\// obtain overlap size
                      Oval_Size = cur.oval_size;
                      //\\//
                      //System.err.println("2 Oval_Size:" + Oval_Size);
			    	  mstr = Node.str_concat(mstr, bstr, Oval_Size);

			    	  merlen = bstr.length() - Oval_Size;
			    	  covsum += ftnode.cov() * bstr.length();
			    	  covlen += merlen;

			    	  //rtnode.addreads(ftnode, shift);

			    	  // we want the same orientation for ftail as before
			    	  if (cur.dir.equals("r")) { mstr = Node.rc(mstr); }
			    	  ftnode.setstr(mstr);

			    	  // Copy reads over
			    	  //ftnode.setR5(rtnode);
			    	  //if (cur.dir.equals("r")) { ftnode.revreads(); }

			    	  ftnode.setCoverage((float) covsum / (float) covlen);
                      
                      ftnode.addAllPairEnd(pairends);

			    	  // Update ftail's new neigbors to be rtail's old neighbors
			    	  // Update the rtail neighbors to point at ftail
			    	  // Update the can compress flags
			    	  // Update threads

			    	  // Clear the old links from ftnode in the direction of the chain
			    	  ftnode.clearEdges(ftail.dir + "f");
			    	  ftnode.clearEdges(ftail.dir + "r");

			    	  // Now move the links from rtnode to ftnode
			    	  for (String adj : Node.dirs)
			    	  {
			    		  String origdir = Node.flip_dir(rtail.dir) + adj;
			    		  String newdir  = ftail.dir + adj;

			    		  //System.err.println("Shifting " + rtail.id + " " + origdir);

			    		  List<String> vl = rtnode.getEdges(origdir);

			    		  if (vl != null)
			    		  {
			    			  for (String v : vl)
			    			  {
                                  //\\//
                                  //output.collect(new Text("rtnode_edge:" + v), new Text("rtnode: " + rtnode.getNodeId() + " ftnode:" + ftnode.getNodeId()));
                                  //output.collect(new Text("rtail_id:" + rtail.id), new Text("ftail_id: " + ftail.id));
                                  int idx = v.indexOf("!");
                                  if (idx == -1) {
                                    throw new IOException("Edge without overlap size information!! ");
                                  }
                                  int v_oval_size = Integer.parseInt(v.substring(idx+1));
			    				  if (v.substring(0, idx).equals(rtail.id))
			    				  {
			    					  // Cycle on rtail
			    					  if (VERBOSE) { System.err.println("Fixing rtail cycle"); }

			    					  String cycled = ftail.dir;

			    					  if (rtail.dir.equals(adj)) { cycled += Node.flip_dir(ftail.dir); }
			    					  else                       { cycled += ftail.dir; }

			    					  ftnode.addEdge(cycled, ftail.id  + "!" + ftail.oval_size);
			    				  }
			    				  else
			    				  {
			    					  ftnode.addEdge(newdir, v);
                                      //output.collect(new Text("ADD"), new Text( newdir + " " + v + " ftnode:" + ftnode.getNodeId()));
                                      //\\//  Update the rtail neighbors to point at ftail
			    					  Node vnode = nodes.get(v.substring(0, idx));
                                      //output.collect(new Text("REPLACE"), new Text( rtail.id + "!" + v_oval_size + " v.s " + ftail.id + "!" + v_oval_size));
			    					  vnode.replacelink(rtail.id + "!" + v_oval_size, Node.flip_link(origdir),
			    							            ftail.id + "!" + v_oval_size, Node.flip_link(newdir));
			    				  }
			    			  }
			    		  }
			    	  }

			        // Now move the can compresflag from rtnode into ftnode
			    	//if (node.getBlackEdges() == null) {
                        ftnode.setCanCompress(ftail.dir, rtnode.canCompress(Node.flip_dir(rtail.dir)));
                    //}

			        // Break cycles
			    	for (String dir : Node.dirs)
			        {
			    		TailInfo next = ftnode.gettail(dir);

			    		if ((next != null) && next.id.equals(ftail.id))
			    		{
			    			if (VERBOSE) { System.err.println("Breaking tail " + ftail.id); }
			    			ftnode.setCanCompress("f", false);
			    			ftnode.setCanCompress("r", false);
			    		}
			        }

			        // Confirm there are no threads in $ftnode in $fdir
			    	/*List<String> threads = ftnode.getThreads();
			    	if (threads != null)
			    	{
			    	  ftnode.clearThreads();

			    	  for (String thread : threads)
			    	  {
			    		  String [] vals = thread.split(":"); // t, link, read

			    		  if (!vals[0].substring(0,1).equals(ftail.dir))
			    		  {
			    			ftnode.addThread(vals[0], vals[1], vals[2]);
			    		  }
			    	  }
			    	}*/

			        // Now copy over rtnodes threads in !$rdir
			    	/*threads = rtnode.getThreads();
			    	if (threads != null)
			    	{
			    		for (String thread : threads)
			    		{
			    			String [] vals = thread.split(":"); // t, link, read
			    			if (!vals[0].substring(0,1).equals(rtail.dir))
			    			{
			    				String et = ftail.dir + vals[0].substring(1);
			    				ftnode.addThread(et, vals[1], vals[2]);
			    			}

			    		}
			    	}*/

			        if (VERBOSE) { System.err.println(ftnode.toNodeMsg(true)); }
			        if (VERBOSE) { System.err.println("==]"); }
			      }  // domerge == 2
			      else
			      {
			        if (mergelen < chainlen)
			        {
			            System.err.println("Hit an unexpected cycle mergelen: " + mergelen + " chainlen: " + chainlen + " in " + rtnode.getNodeId() + " " + ftnode.getNodeId() + " mertag:" + mertag.toString());
			            System.err.println(rtnode.toNodeMsg(true));
			            System.err.println(ftnode.toNodeMsg(true));
			            throw new IOException("Hit an unexpected cycle mergelen: " + mergelen + " chainlen: " + chainlen + " in " + rtnode.getNodeId() + " " + ftnode.getNodeId() + " mertag:" + mertag.toString());
			        }

			        if (mergedir.equals("r"))
			        {
			        	mstr = Node.rc(mstr);
			        	//rtnode.revreads();
			        }

			        rtnode.setstr(mstr);

			        rtnode.setCoverage((float) covsum / (float) covlen);
                    //\\
                    rtnode.addAllPairEnd(pairends);
                    //\\
			        String mergeftaildir = lastdir;
			        if (!lastdir.equals(mergedir)) { mergeftaildir = Node.flip_dir(mergeftaildir); }

			        // update rtail->first with rtail->ftail link
			        rtnode.replacelink(first.id + "!" + Oval_Size, mergedir + first.dir,
			        		           ftail.id + "!" + Oval_Size, mergeftaildir + cur.dir);

			        ftnode.replacelink(lastid + "!" + Oval_Size, Node.flip_link(lastdir+cur.dir),
			        		           rtail.id + "!" + Oval_Size,Node.flip_link(mergeftaildir + cur.dir));

			       /* if (curnode.getThreads() != null)
			        {
			        	//throw new IOException("ERROR: curnode has threads " + curnode.toNodeMsg(true));
			        	curnode.cleanThreads();
			        }*/

			        if (VERBOSE) { System.err.println(rtnode.toNodeMsg(true)); }
			        if (VERBOSE) { System.err.println("==]"); }
			      }

			      saved  += mergelen;
			      cchains++;
			    }
			}

			for(String nodeid : nodes.keySet())
			{
				Node node = nodes.get(nodeid);
				if (node.hasCustom(DONE) && node.getCustom(DONE).get(0).equals("1"))
				{
					output.collect(new Text(node.getNodeId()), new Text(node.toNodeMsg()));
				}
			}

			reporter.incrCounter("Brush", "chains",        chains);
			reporter.incrCounter("Brush", "cchains",       cchains);
		    reporter.incrCounter("Brush", "totalchainlen", totallen);
		    reporter.incrCounter("Brush", "saved",         saved);
		}
	}




	public RunningJob run(String inputPath, String outputPath) throws Exception
	{
		sLogger.info("Tool name: QuickMerge");
		sLogger.info(" - input: "  + inputPath);
		sLogger.info(" - output: " + outputPath);

		//JobConf conf = new JobConf(Stats.class);
        JobConf conf = new JobConf(QuickMerge.class);
		conf.setJobName("QuickMerge " + inputPath + " " + BrushConfig.K);

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
        
		conf.setMapperClass(QuickMergeMapper.class);
		conf.setReducerClass(QuickMergeReducer.class);

		//delete the output directory if it exists already
		FileSystem.get(conf).delete(new Path(outputPath), true);

		return JobClient.runJob(conf);
	}


	public int run(String[] args) throws Exception
	{
		String inputPath  = "";
		String outputPath = "";
		BrushConfig.K = 29;

		//BrushConfig.hadoopBasePath = "foo";
		//BrushConfig.hadoopReadPath = "foo";

		run(inputPath, outputPath);
		return 0;
	}

	public static void main(String[] args) throws Exception
	{
		int res = ToolRunner.run(new Configuration(), new QuickMerge(), args);
		System.exit(res);
	}
}
