/*
    MatchPrefix.java
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Set;
import java.util.List;
import java.util.Map;

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


public class MatchPrefix extends Configured implements Tool
{
	private static final Logger sLogger = Logger.getLogger(MatchPrefix.class);

	public static class MatchPrefixMapper extends MapReduceBase
    implements Mapper<LongWritable, Text, Text, Text>
	{
		public static int K = 0;
		public static int TRIM5 = 0;
		public static int TRIM3 = 0;

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
            // clean edge for Build Second
            for (String key : Node.edgetypes)
			{
                node.clearEdges(key);
            }
            
            if (!node.hasCustom("n")){
                String prefix_tmp = node.str().substring(0, K);
                String prefix = Node.str2dna(prefix_tmp);

                String prefix_rc_tmp = Node.rc(node.str().substring(node.len()-K));
                String prefix_rc = Node.str2dna(prefix_rc_tmp);
                //output.collect(new Text(prefix), new Text(node.getNodeId() + "\t"  + "f" + "\t" + Node.NODEMSG + "\t" + node.len() + "\t" + Node.str2dna(node.str().substring(K)) +  "\t" + node.cov() ));
                //output.collect(new Text(prefix_rc), new Text(node.getNodeId() + "\t" + "r" + "\t"+ Node.NODEMSG + "\t" + node.len() + "\t" + Node.str2dna(node.str().substring(0, node.len()-K)) + "\t" + node.cov() ));
                output.collect(new Text(prefix), new Text(node.getNodeId() + "\t"  + "f" + "\t" + node.toNodeMsg() ));
                output.collect(new Text(prefix_rc), new Text(node.getNodeId() + "\t" + "r" + "\t"+ node.toNodeMsg() ));
                reporter.incrCounter("Brush", "nodes", 1);
                //slide the K-mer windows for each read in both strands
                int end = node.len() - K;
                for (int i = 1; i < end; i++)
                {
                    String window_tmp = node.str().substring(i,   i+K);
                    String window_r_tmp = Node.rc(node.str().substring(node.len() - K - i, node.len() - i));
                    //String window_r_tmp = Node.rc(window_tmp);
                    String window = Node.str2dna(window_tmp);
                    String window_r = Node.str2dna(window_r_tmp);
                    //int remained_base = node.len() - K - i;
                    int overlap_size_f = node.len() - i;
                    int overlap_size_r = node.len() - i;
                    //int overlap_size_r = node.len() - i;
                    if (overlap_size_f >= K && overlap_size_f <= node.len() && !window_tmp.matches("A*") && !window_tmp.matches("T*") ) {
                        output.collect(new Text(window),
                                       new Text(node.getNodeId() + "\t" + "f" + "\t" + Node.SUFFIXMSG + "\t" + overlap_size_f + "\t" + node.cov()));
                    }
                    if (overlap_size_r >= K && overlap_size_r <= node.len() && !window_tmp.matches("A*") && !window_tmp.matches("T*") ) {
                        output.collect(new Text(window_r),
                                       new Text(node.getNodeId() + "\t" + "r" + "\t" + Node.SUFFIXMSG + "\t" + overlap_size_r + "\t" + node.cov()));
                    }
                }
            }
		}
	}

	public static class MatchPrefixReducer extends MapReduceBase
	implements Reducer<Text, Text, Text, Text>
	{
		private static int K = 0;
        //private static int OVALSIZE = 0;
        private static long All_Kmer = 0;
        private static long Diff_Kmer = 0;
        private static long HighKmer = 0;
        private static long LowKmer = 0;
        private static long All_Reads = 0;
        //private static float TF_IDF = 0;

		public void configure(JobConf job) {
			K = Integer.parseInt(job.get("K"));
            All_Kmer = Long.parseLong(job.get("AllKmer"));
            Diff_Kmer = Long.parseLong(job.get("DiffKmer"));
            HighKmer = Long.parseLong(job.get("UP_KMER"));
            LowKmer = Long.parseLong(job.get("LOW_KMER"));
            All_Reads = Long.parseLong(job.get("AllReads"));
            //TF_IDF = Float.parseFloat(job.get("TFIDF"));
		}

        public class EdgeInfo
		{
			public String id;
            public String edge_type;
			public int overlap_size;
            public float cov;

			public EdgeInfo(String id1, String edge_type1, int overlap_size1, float cov1) throws IOException
			{
				id = id1;
                edge_type = edge_type1;
                overlap_size = overlap_size1;
                cov = cov1;
			}

            public String toString()
			{
				return id + "!" + overlap_size + "|" + cov;
			}
		}
        class OvelapSizeComparator implements Comparator {
            public int compare(Object element1, Object element2) {
                EdgeInfo obj1 = (EdgeInfo) element1;
                EdgeInfo obj2 = (EdgeInfo) element2;
                if ((int)(obj1.overlap_size - obj2.overlap_size) >= 0) {
                    return -1;
                } else {
                    return 1;
                }
            }
        }

		public void reduce(Text prefix, Iterator<Text> iter,
						   OutputCollector<Text, Text> output, Reporter reporter)
						   throws IOException
		{
            Map<String, Node> nodes = new HashMap<String, Node>();
			List<EdgeInfo> elist = new ArrayList<EdgeInfo>();
            Map<String, List<String>> edges_list = new HashMap<String, List<String>>();

            int prefix_sum = 0;
            int belong_read = 0;
            int kmer_count = 0;
            List<String> ReadID_list = new ArrayList<String>();
            while(iter.hasNext())
			{
				String msg = iter.next().toString();

				String [] vals = msg.split("\t");
                float tmp_cov=0;
				if (vals[2].equals(Node.NODEMSG))
				{
                    String edge_type = vals[1];
                    String edge_content= vals[0] + "!" + vals[3];
                    Node node = new Node(vals[0]);
                    /*String seq;
                    if (edge_type.equals("r")){
                        seq = Node.dna2str(vals[4]) + Node.rc(Node.dna2str(prefix.toString()));
                    } else {
                        seq = Node.dna2str(prefix.toString()) + Node.dna2str(vals[4]);
                    }
                    node.setstr(seq);
                    node.setCoverage(Float.parseFloat(vals[5]));*/
                    node.parseNodeMsg(vals, 2);
                    nodes.put(node.getNodeId() + "|" + edge_type, node);

                    //use as edge info
                    //tmp_cov = Float.parseFloat(vals[5]);
                    //EdgeInfo ei = new EdgeInfo(vals[0], vals[1], Integer.parseInt(vals[3]), Float.parseFloat(vals[5]));
                    EdgeInfo ei = new EdgeInfo(vals[0], vals[1], node.len(), node.cov());
                    elist.add(ei);
                    if (ReadID_list.contains(ei.id)){
                        // do nothing
                    } else {
                        belong_read = belong_read + 1;
                        ReadID_list.add(ei.id);
                    }
                 /*   if (edges_list.containsKey(edge_type))
                    {
                        edges_list.get(edge_type).add(edge_content);
                    }
                    else
                    {
                        List<String> tmp_edges = null;
                        tmp_edges = new ArrayList<String>();
                        tmp_edges.add(edge_content);
                        edges_list.put(edge_type, tmp_edges);
                    }*/
				}
				else if (vals[2].equals(Node.SUFFIXMSG))
				{
                    //\\// ReadID + "\t" + "r" + "\t" + Node.SUFFIXMSG  + "\t" + overlap_size
                    tmp_cov = Float.parseFloat(vals[4]);
                    EdgeInfo ei = new EdgeInfo(vals[0], vals[1], Integer.parseInt(vals[3]), Float.parseFloat(vals[4]));
                    elist.add(ei);
                    if (ReadID_list.contains(ei.id)){
                        // do nothing
                    } else {
                        belong_read = belong_read + 1;
                        ReadID_list.add(ei.id);
                    }
				}
				else
				{
					throw new IOException("Unknown msgtype: " + msg);
				}
                kmer_count = kmer_count + (int)tmp_cov;
				prefix_sum = prefix_sum + (int)tmp_cov;
			}
            if (prefix_sum > LowKmer /*&& prefix_sum < HighKmer*/ ){
                 Collections.sort(elist, new OvelapSizeComparator());
                 for (int i = 0; i < elist.size() && i < HighKmer; i++){
                    if (edges_list.containsKey(elist.get(i).edge_type))
                    {
                        edges_list.get(elist.get(i).edge_type).add(elist.get(i).toString());
                    }
                    else
                    {
                        List<String> tmp_edges = null;
                        tmp_edges = new ArrayList<String>();
                        tmp_edges.add(elist.get(i).toString());
                        edges_list.put(elist.get(i).edge_type, tmp_edges);
                    }
                }
            } /*else {
                Collections.sort(elist, new OvelapSizeComparator());
                for (int i = 0; i < elist.size(); i++){
                    if (edges_list.containsKey(elist.get(i).edge_type))
                    {
                        edges_list.get(elist.get(i).edge_type).add(elist.get(i).toString());
                    }
                    else
                    {
                        List<String> tmp_edges = null;
                        tmp_edges = new ArrayList<String>();
                        tmp_edges.add(elist.get(i).toString());
                        edges_list.put(elist.get(i).edge_type, tmp_edges);
                    }
                }
            }*/

            for(String nodeid_dir : nodes.keySet())
			{
                String dir = nodeid_dir.substring(nodeid_dir.indexOf("|")+1);
				Node node = nodes.get(nodeid_dir);
                //output.collect(new Text(node.getNodeId()), new Text(nodeid_dir));
                // K-mer frequence filter
                //\\\\\\\\\\\\\\\\\\\\\\\\\\\\\
                for(String adj : Node.dirs)
                {
                    List<String> edges = edges_list.get(adj);
                    if (edges != null)
                    {
                        String key = Node.flip_dir(dir) + Node.flip_dir(adj);
                        for(int i=0; i < edges.size(); i++){
                            String edge_id = edges.get(i).substring(0, edges.get(i).indexOf("!"));
                            int overlap_size = Integer.parseInt(edges.get(i).substring(edges.get(i).indexOf("!")+1, edges.get(i).indexOf("|")));
                            float cov = Float.parseFloat(edges.get(i).substring(edges.get(i).indexOf("|")+1));
                            //output.collect(new Text(node.getNodeId()), new Text( key + "\t" + edge_id + "\t" + node.len() + "\t" + overlap_size));
                            if (node.getNodeId().equals(edge_id) ) {
                                // do not add self
                                //output.collect(new Text("XXX"), new Text(edge_id + "\t" + node.len() + "\t" + overlap_size));
                            } else {
                                node.addEdge(key, edges.get(i).substring(0,edges.get(i).indexOf("|")));
                               // output.collect(new Text(Math.log(10+node.cov()+cov)*Math.log((double)All_Reads/(double)belong_read)+""), new Text(Math.log(10*node.cov()*cov)*Math.log((double)All_Reads/(double)belong_read)+""));
                            }
                        }
                    }
                }
                //\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\
               
                
                output.collect(new Text(node.getNodeId()), new Text(node.toNodeMsg()));
                reporter.incrCounter("Brush", "nodecount", 1);
			}
           //memory clean
           //nodes.clear();
           //edges_list.clear();
		}
	}



	public RunningJob run(String inputPath, String outputPath, long allkmer, long diffkmer, long nodes) throws Exception
	{
		sLogger.info("Tool name: MatchPrefix");
		sLogger.info(" - input: "  + inputPath);
		sLogger.info(" - output: " + outputPath);

		JobConf conf = new JobConf(MatchPrefix.class);
		conf.setJobName("MatchPrefix " + inputPath + " " + BrushConfig.K);
        conf.setLong("AllKmer", allkmer);
        conf.setLong("DiffKmer", diffkmer);
        conf.setLong("AllReads", nodes);

		BrushConfig.initializeConfiguration(conf);

		FileInputFormat.addInputPath(conf, new Path(inputPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(MatchPrefixMapper.class);
		conf.setReducerClass(MatchPrefixReducer.class);

		//delete the output directory if it exists already
		FileSystem.get(conf).delete(new Path(outputPath), true);

		return JobClient.runJob(conf);
	}

	public int run(String[] args) throws Exception
	{
		String inputPath  = "";
		String outputPath = "";
		BrushConfig.K = 21;

		long starttime = System.currentTimeMillis();

		run(inputPath, outputPath, 1, 1, 1);

		long endtime = System.currentTimeMillis();

		float diff = (float) (((float) (endtime - starttime)) / 1000.0);

		System.out.println("Runtime: " + diff + " s");

		return 0;
	}

	public static void main(String[] args) throws Exception
	{
		int res = ToolRunner.run(new Configuration(), new MatchPrefix(), args);
		System.exit(res);
	}
}

