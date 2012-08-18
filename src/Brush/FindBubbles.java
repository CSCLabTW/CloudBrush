/*
    FindBubbles.java
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
import java.util.Collections;
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


public class FindBubbles extends Configured implements Tool
{
	private static final Logger sLogger = Logger.getLogger(FindBubbles.class);

	public static int _min2(int a, int b)
	{
		return (a<b) ? a : b;
	}

	public static int _max2(int a, int b)
	{
		return (a>b) ? a : b;
	}

	public static int _min3(int a, int b, int c)
	{
		  return a < b
		          ? a < c ? a : c
		          : b < c ? b : c;
	}

	public static int fastdistance(String word1, String word2)
	{
		int len1 = word1.length();
		int len2 = word2.length();

		int[][] d = new int[len1+1][len2+1];

		for (int i = 0; i <= len1; i++)
		{
			d[i][0] = i;
		}

		for (int j = 0; j <= len2; j++)
		{
			d[0][j] = j;
		}

		for (int i = 1; i <= len1; i++)
		{
			char w1 = word1.charAt(i-1);
			for (int j = 1; j <= len2; j++)
			{
				char w2 = word2.charAt(j-1);
				int e = (w1 == w2) ? 0 : 1;

				d[i][j] = _min3(d[i-1][j]+1, d[i][j-1]+1, d[i-1][j-1]+e);
			}
		}

		return d[len1][len2];
	}



	// FindBubblesMapper
	///////////////////////////////////////////////////////////////////////////

	public static class FindBubblesMapper extends MapReduceBase
    implements Mapper<LongWritable, Text, Text, Text>
	{
		private static int K = 0;
		public static int MAXBUBBLELEN = 0;

		public void configure(JobConf job)
		{
			K = Integer.parseInt(job.get("K"));
			MAXBUBBLELEN = Integer.parseInt(job.get("MAXBUBBLELEN"));
		}

		public void map(LongWritable lineid, Text nodetxt,
				OutputCollector<Text, Text> output, Reporter reporter)
		throws IOException
		{
			Node node = new Node();
			node.fromNodeMsg(nodetxt.toString());

			output.collect(new Text(node.getNodeId()), new Text(node.toNodeMsg()));
			reporter.incrCounter("Brush", "nodes", 1);
            //\\//
            boolean bubble_msg = false;
            int fdegree = node.degree("f");
			int rdegree = node.degree("r");
            if ((fdegree == 1) && (rdegree == 1)) {
                TailInfo ftail = node.gettail("f");
                TailInfo rtail = node.gettail("r");
                if ( (node.len() - ftail.oval_size - rtail.oval_size) <= MAXBUBBLELEN   ) {
                    bubble_msg = true;
                }
            }
			//\\//
            if (bubble_msg/*node.len() < MAXBUBBLELEN*/)
			{
				if ((fdegree == 1) && (rdegree == 1))
				{
					reporter.incrCounter("Brush", "potentialbubbles", 1);
                    //int fdegree = node.degree("f");
			        //int rdegree = node.degree("r");
					TailInfo ftail = node.gettail("f");
					TailInfo rtail = node.gettail("r");

					String major  = ftail.id;
					String majord = "f" + ftail.dir;
                    int major_oval = ftail.oval_size;

					String minor  = rtail.id;
					String minord = "r" + rtail.dir;
                    int minor_oval = rtail.oval_size;

					//\\// 閮剖�bubble���湔�
                    if (rtail.id.compareTo(ftail.id) > 0)
					{
						String tmpid = major;
						String tmpdir = majord;
                        int tmp_oval = major_oval;

						major = minor;
						majord = minord;
                        major_oval = minor_oval;

						minor = tmpid;
						minord = tmpdir;
                        minor_oval = tmp_oval;
					}

					majord = Node.flip_link(majord);
					minord = Node.flip_link(minord);

					String str = node.str();
                    //System.err.println(rtail.oval_size + " n_len:" + node.len() + " f:" + ftail.oval_size);
                    if (node.len()-ftail.oval_size < rtail.oval_size) {
                        str = "";
                    } else {
                        str = str.substring(rtail.oval_size, node.len()-ftail.oval_size);
                    }
                    str = Node.str2dna(str);
					float cov = node.cov();

					output.collect(new Text(major),
							new Text(Node.BUBBLELINKMSG + "\t" +
									majord + "\t" + node.getNodeId() + "\t" +
									minord + "\t" + minor + "\t" +
									str + "\t" + cov + "\t" + major_oval + "\t" +minor_oval + "\t" + node.len()));

					//print "$major\t$BUBBLELINKMSG\t$majord\t$nodeid\t$minord\t$minor\t$str\t$cov\n";
				}
			}
		}
	}


	// FindBubblesReducer
	///////////////////////////////////////////////////////////////////////////

	public static class FindBubblesReducer extends MapReduceBase
	implements Reducer<Text, Text, Text, Text>
	{
		private static int K = 0;
        static public boolean VERBOSE = false;
        static public float BUBBLEEDITRATE = 0.05f;

		public void configure(JobConf job) {
			K = Integer.parseInt(job.get("K"));
			BUBBLEEDITRATE = Float.parseFloat(job.get("BUBBLEEDITRATE"));
		}

		public class BubbleInfo implements Comparable
		{
			public String dir;
			public String id;
			public String minord;
			public String minor;
			public String str;
			public float  cov;
			public boolean popped;
            //\\\\\ for string graph
            public int oval;
            public int minor_oval;
            public int len;

			public BubbleInfo(String[] vals, int offset) throws IOException
			{
				//print "$major\t$BUBBLELINKMSG\t$majord\t$nodeid\t$minord\t$minor\t$str\t$cov\n";

				if (!vals[offset].equals(Node.BUBBLELINKMSG))
				{
					throw new IOException("Unknown message type");
				}

				dir = vals[offset+1];
				id  = vals[offset+2];
				minord = vals[offset+3];
				minor  = vals[offset+4];
				str    = vals[offset+5];
				cov    = Float.parseFloat(vals[6]);
				popped = false;
                //\\ for string graph
                oval = Integer.parseInt(vals[7]);
                minor_oval = Integer.parseInt(vals[8]);
                len = Integer.parseInt(vals[9]);
			}

			public String toString()
			{
				return dir + " " + id + " " + minord + " " + minor + " " + cov + " " + str + " " + oval + " " + minor_oval + " " + len;
			}

			public int compareTo(Object o)
			{
				BubbleInfo co = (BubbleInfo) o;
				return (int)((co.cov - cov)*100);
			}
		}


		public void reduce(Text nodeid, Iterator<Text> iter,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException
		{
			Node node = new Node(nodeid.toString());
			Map<String, List<BubbleInfo>> bubblelinks = new HashMap<String, List<BubbleInfo>>();

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
				else if (vals[0].equals(Node.BUBBLELINKMSG))
				{
					BubbleInfo bi = new BubbleInfo(vals, 0);
					reporter.incrCounter("Brush", "linkschecked", 1);

					if (!bubblelinks.containsKey(bi.minor))
					{
						List<BubbleInfo> blist = new ArrayList<BubbleInfo>();
						blist.add(bi);
						bubblelinks.put(bi.minor, blist);
					}
					else
					{
						bubblelinks.get(bi.minor).add(bi);
					}
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

			if (bubblelinks.size() > 0)
			{
				int popped = 0;

				for (String minor : bubblelinks.keySet())
				{
					List<BubbleInfo> interior = bubblelinks.get(minor);

					int choices = interior.size();
					reporter.incrCounter("Brush", "minorchecked", 1);
                   
					if (choices > 1)
					{
						reporter.incrCounter("Brush", "edgeschecked", choices);

						// Sort potential bubble strings in order of decreasing coverage
						Collections.sort(interior);
                        //divide four type maj(0,1) + min(0,1) = "ff", "rr", "fr", "rf"
                        //\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\
                        Map<String, List<BubbleInfo>> sub_interior = new HashMap<String, List<BubbleInfo>>();
                        for (int i = 0; i < choices; i++)
						{
                            BubbleInfo tmp = interior.get(i);
                            if ((tmp.dir.substring(0,1)+tmp.minord.substring(0,1)).equals("ff")){
                                if (sub_interior.containsKey("ff")) {
                                    sub_interior.get("ff").add(tmp);
                                } else {
                                    List<BubbleInfo> tmp_interior = new ArrayList<BubbleInfo>();
                                    tmp_interior.add(tmp);
                                    sub_interior.put("ff", tmp_interior);
                                }
                            } else if ((tmp.dir.substring(0,1)+tmp.minord.substring(0,1)).equals("rr")) {
                                if (sub_interior.containsKey("rr")) {
                                    sub_interior.get("rr").add(tmp);
                                } else {
                                    List<BubbleInfo> tmp_interior = new ArrayList<BubbleInfo>();
                                    tmp_interior.add(tmp);
                                    sub_interior.put("rr", tmp_interior);
                                }
                            } else if ((tmp.dir.substring(0,1)+tmp.minord.substring(0,1)).equals("fr")) {
                                if (sub_interior.containsKey("fr")) {
                                    sub_interior.get("fr").add(tmp);
                                } else {
                                    List<BubbleInfo> tmp_interior = new ArrayList<BubbleInfo>();
                                    tmp_interior.add(tmp);
                                    sub_interior.put("fr", tmp_interior);
                                }
                            } else if ((tmp.dir.substring(0,1)+tmp.minord.substring(0,1)).equals("rf")) {
                                if (sub_interior.containsKey("rf")) {
                                    sub_interior.get("rf").add(tmp);
                                } else {
                                    List<BubbleInfo> tmp_interior = new ArrayList<BubbleInfo>();
                                    tmp_interior.add(tmp);
                                    sub_interior.put("rf", tmp_interior);
                                }
                            }
                        }
                        // for each sub interior
                        for (String key : Node.edgetypes){
                            List<BubbleInfo> s_interior = null;
                            s_interior  = sub_interior.get(key);
                            int s_choices = 0;
                            if (s_interior != null) {
                                s_choices = s_interior.size();
                            } 
                            //\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\
                            for (int i = 0; i < s_choices; i++)
                            {
                                BubbleInfo u = s_interior.get(i);
                                if (u.popped) { continue; }

                                String ustr = Node.dna2str(u.str);

                                for (int j = i+1; j < s_choices; j++)
                                {
                                    BubbleInfo v = s_interior.get(j);
                                    if (v.popped) { continue; }

                                    String vstr = Node.dna2str(v.str);

                                    if ((!u.dir.equals(v.dir)) && (!u.dir.equals(Node.flip_link(v.dir))))
                                    {
                                        vstr = Node.rc(vstr);
                                    }

                                    //output.collect(new Text("[" + ustr + "]"), new Text("[" + vstr + "]"));
                                    int distance = fastdistance(ustr, vstr);
                                    int threshold = (int)(_max2(ustr.length(), vstr.length()) * BUBBLEEDITRATE);
                                    //int threshold = (int)(node.len() * BUBBLEEDITRATE);

                                    reporter.incrCounter("Brush", "bubbleschecked", 1);

                                    if (VERBOSE)
                                    {
                                        System.err.println("Bubble comparison:\n" + u.id +"\t" + ustr + "\n" + v.id + "\t" + vstr);
                                        System.err.println("edit distance: " + distance + " threshold:" + threshold);
                                    }

                                    if (distance <= threshold)
                                    {
                                        // Found a bubble!

                                        if (VERBOSE)
                                        {
                                            System.err.println("POP " + node.getNodeId() + " " + u.id + " " + v.id);
                                        }

                                        v.popped = true;

                                        popped++;
                                        reporter.incrCounter("Brush", "poppedbubbles", 1);

                                        int vmerlen = vstr.length()/* - K + 1*/;
                                        float extracov = v.cov * vmerlen;

                                        node.addBubble(minor, v.minord, v.id, u.minord, u.id, extracov, 0);
                                        node.removelink(v.id, v.dir);
                                        //node.updateThreads(v.minord, v.id, u.dir, u.id);
                                    }
                                }
                            }
                            //\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\
                        }
						// See if there are any pairwise compatible strings
						/*for (int i = 0; i < choices; i++)
						{
							BubbleInfo u = interior.get(i);
							if (u.popped) { continue; }

							String ustr = Node.dna2str(u.str);

							for (int j = i+1; j < choices; j++)
							{
								BubbleInfo v = interior.get(j);
								if (v.popped) { continue; }

								String vstr = Node.dna2str(v.str);

								if ((!u.dir.equals(v.dir)) && (!u.dir.equals(Node.flip_link(v.dir))))
								{
									vstr = Node.rc(vstr);
								}

                                //output.collect(new Text("[" + ustr + "]"), new Text("[" + vstr + "]"));
								int distance = fastdistance(ustr, vstr);
								//int threshold = (int)(_max2(ustr.length(), vstr.length()) * BUBBLEEDITRATE);
                                int threshold = (int)(node.len() * BUBBLEEDITRATE);

								reporter.incrCounter("Brush", "bubbleschecked", 1);

								if (VERBOSE)
								{
									System.err.println("Bubble comparison:\n" + u.id +"\t" + ustr + "\n" + v.id + "\t" + vstr);
									System.err.println("edit distance: " + distance + " threshold:" + threshold);
								}

								if (distance <= threshold)
								{
									// Found a bubble!

									if (VERBOSE)
									{
										System.err.println("POP " + node.getNodeId() + " " + u.id + " " + v.id);
									}

									v.popped = true;

									popped++;
									reporter.incrCounter("Brush", "poppedbubbles", 1);

									int vmerlen = vstr.length();
									float extracov = v.cov * vmerlen;

									node.addBubble(minor, v.minord, v.id, u.minord, u.id, extracov, 0);
									node.removelink(v.id, v.dir);
									//node.updateThreads(v.minord, v.id, u.dir, u.id);
								}
							}
						}*/
					}
			    }

			    if (popped > 0)
			    {
			    	//node.cleanThreads();
			    }
			}

			output.collect(nodeid, new Text(node.toNodeMsg()));
		}
	}




	// Run Tool
	///////////////////////////////////////////////////////////////////////////

	public RunningJob run(String inputPath, String outputPath) throws Exception
	{
		sLogger.info("Tool name: FindBubbles");
		sLogger.info(" - input: "  + inputPath);
		sLogger.info(" - output: " + outputPath);

		JobConf conf = new JobConf(FindBubbles.class);
		conf.setJobName("FindBubbles " + inputPath + " " + BrushConfig.K);

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

		conf.setMapperClass(FindBubblesMapper.class);
		conf.setReducerClass(FindBubblesReducer.class);

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
		int res = ToolRunner.run(new Configuration(), new FindBubbles(), args);
		System.exit(res);
	}
}

