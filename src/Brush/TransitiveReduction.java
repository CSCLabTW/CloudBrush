/*
    TransitiveReduction.java
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


public class TransitiveReduction extends Configured implements Tool
{
	private static final Logger sLogger = Logger.getLogger(TransitiveReduction.class);

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
    // TransitiveReductionMapper
	///////////////////////////////////////////////////////////////////////////

	public static class TransitiveReductionMapper extends MapReduceBase
    implements Mapper<LongWritable, Text, Text, Text>
	{
		public void map(LongWritable lineid, Text nodetxt,
                OutputCollector<Text, Text> output, Reporter reporter)
                throws IOException
        {
			Node node = new Node();
			node.fromNodeMsg(nodetxt.toString());

			for (String key : Node.edgetypes)
            {
                List<String> edges = node.getEdges(key);
                if (edges != null)
                {
                    for(int i = 0; i < edges.size(); i++){
                        String [] vals = edges.get(i).split("!");
                        String edge_id = vals[0];
                        String oval_size = vals[1];
                        //String con = Node.flip_dir(adj) + "f";
                        String con = Node.flip_link(key);
                        output.collect(new Text(edge_id), new Text(Node.OVALMSG + "\t" + node.getNodeId() + "\t" + node.str_raw() + "\t" + con + "\t" + oval_size));
                        //\\// emit reverse edge
                        //output.collect(new Text(edge_id), new Text(Node.OVALMSG + "\t" + node.getNodeId() + "\t" + node.str_raw() + "\t" + key + "\t" + oval_size));
                    }
                }
			}
            List<String> emit_node = new ArrayList<String>();
            output.collect(new Text(node.getNodeId()), new Text(node.toNodeMsg()));
			reporter.incrCounter("Brush", "nodes", 1);
        }
	}


    // TransitiveReductionReducer
	///////////////////////////////////////////////////////////////////////////

	public static class TransitiveReductionReducer extends MapReduceBase
	implements Reducer<Text, Text, Text, Text>
	{
        private static int K = 0;
        static public float ERRORRATE = 0.00f;

		public void configure(JobConf job) {
			K = Integer.parseInt(job.get("K"));
            //ERRORRATE = Float.parseFloat(job.get("ERRORRATE"));
		}

        public class OverlapInfo
		{
			public String id;
            public String str;
            public String edge_type;  //隞�node 撠迨overlap�����e.g. node [r,f]f overlap
			public int overlap_size;

			public OverlapInfo(String[] vals, int offset) throws IOException
			{
				//print "$major\t$BUBBLELINKMSG\t$majord\t$nodeid\t$minord\t$minor\t$str\t$cov\n";

				if (!vals[offset].equals(Node.OVALMSG))
				{
					throw new IOException("Unknown message type");
				}
				id = vals[offset+1];
				str  = vals[offset+2];
                edge_type = vals[offset+3];
                overlap_size = Integer.parseInt(vals[offset+4]);

			}

            public String toString()
			{
				return edge_type + " " + id + " " + overlap_size + " " + str;
			}
		}

        class OvelapSizeComparator_f implements Comparator {
            public int compare(Object element1, Object element2) {
                OverlapInfo obj1 = (OverlapInfo) element1;
                OverlapInfo obj2 = (OverlapInfo) element2;
                if ((int)(obj1.overlap_size - obj2.overlap_size) > 0) {
                    return -1;
                } else if ((int)(obj1.overlap_size - obj2.overlap_size) < 0){
                    return 1;
                } else {
                    if (obj1.str.length() - obj2.str.length() < 0) {
                        return -1;
                    } else if (obj1.str.length() - obj2.str.length() > 0) {
                        return 1;
                    } else {
                        if ( obj1.id.compareTo(obj2.id) < 0) {
                            return -1;
                        } else {
                            return 1;
                        }
                    }
                }
            }
        }

        class OvelapSizeComparator_r implements Comparator {
            public int compare(Object element1, Object element2) {
                OverlapInfo obj1 = (OverlapInfo) element1;
                OverlapInfo obj2 = (OverlapInfo) element2;
                if ((int)(obj1.overlap_size - obj2.overlap_size) > 0) {
                    return -1;
                } else if ((int)(obj1.overlap_size - obj2.overlap_size) < 0){
                    return 1;
                } else {
                    if (obj1.str.length() - obj2.str.length() < 0) {
                        return -1;
                    } else if (obj1.str.length() - obj2.str.length() > 0) {
                        return 1;
                    } else {
                        if ( obj1.id.compareTo(obj2.id) < 0) {
                            return 1;
                        } else {
                            return -1;
                        }
                    }
                }
            }
        }

        public class Prefix {
            public String id;
            public String suffix;
            public String str;
            public String edge_type;
            public int oval_size;
            public Prefix(String id1, String edge_type1, String str1, int oval_size1){
                id = id1;
                edge_type = edge_type1;
                str = str1;
                oval_size = oval_size1;
                suffix = str1.substring(oval_size);
            }
        }


		public void reduce(Text nodeid, Iterator<Text> iter,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException
		{
			Node node = new Node(nodeid.toString());
		    //List<OverlapInfo> olist = new ArrayList<OverlapInfo>();
            List<OverlapInfo> o_flist = new ArrayList<OverlapInfo>();
            List<OverlapInfo> o_rlist = new ArrayList<OverlapInfo>();

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
				else if (vals[0].equals(Node.OVALMSG))
				{
					OverlapInfo oi = new OverlapInfo(vals, 0);
                    //olist.add(oi);
                    if (oi.edge_type.charAt(0) == 'f') {
                        o_flist.add(oi);
                    } else if (oi.edge_type.charAt(0) == 'r'){
                        o_rlist.add(oi);
                    }
                    //output.collect(new Text(node.getNodeId()), new Text("X" + "\t" + oi.id ));
				}
				else
				{
					throw new IOException("Unknown msgtype: " + msg);
				}
			}
            //\\ one node
			if (sawnode != 1)
			{
				throw new IOException("ERROR: Didn't see exactly 1 nodemsg (" + sawnode + ") for " + nodeid.toString());
			}

            //\\ store confirmed edges
            Map<String, List<String>> edges_list = new HashMap<String, List<String>>();
            Map<String, List<String>> IDs_flist = new HashMap<String, List<String>>();
            Map<String, List<String>> IDs_rlist = new HashMap<String, List<String>>();
            Map<String, List<Prefix>> PREFIXs_list = new HashMap<String, List<Prefix>>();

           
            //\\\\\\\\\\\\\\\\\\\\\\\\\\ f_overlap
            int f_choices = o_flist.size();
            if (f_choices > 0)
			{
                // Sort overlap strings in order of decreasing overlap size
                Collections.sort(o_flist, new OvelapSizeComparator_f());
                //output.collect(new Text(node.getNodeId()), new Text(olist.size() + "!"));
                // See if there are any pairwise compatible strings
                for (int i = 0; i < f_choices; i++)
                {
                    String oval_id = o_flist.get(i).id;
                    String oval_type = o_flist.get(i).edge_type;
                    //String node_dir = oval_type.substring(0, 1);
                    String oval_dir = oval_type.substring(1);
                    int oval_size = o_flist.get(i).overlap_size;
                    String edge_content = oval_id + "!" + oval_size;

                    String oval_seq_tmp = Node.dna2str(o_flist.get(i).str);
                    String oval_seq;
                    if (oval_dir.equals("r")){
                        oval_seq = Node.rc(oval_seq_tmp);
                    } else {
                        oval_seq = oval_seq_tmp;
                    }

                    //output.collect(new Text(node.getNodeId()), new Text("A" + "\t" + olist.get(i).id + "\t" + node.toNodeMsg()));
                    //\\ Self contained filter
                    if (oval_size == oval_seq.length() && oval_size == node.str().length() ) {
                        //node.addRemovalEdge(oval_id, Node.flip_link(oval_type), node.getNodeId(), oval_size);
                        reporter.incrCounter("Brush", "contained_edge", 1);
                        //continue;
                    }
                    //\\\\\\\\\\\ Maximal Overlap filter
                    List<String> stored_IDs = IDs_flist.get(oval_type);
                    boolean has_large_overlap = false;
                    if (stored_IDs != null && stored_IDs.contains(oval_id))
                    {
                        has_large_overlap = true;
                    }
                    if (has_large_overlap){
                        node.addRemovalEdge(oval_id, Node.flip_link(oval_type), node.getNodeId(), oval_size);
                        continue;
                    }

                   //\\\\\\\\\\\ Transitive Reduction filter
                    List<Prefix> stored_PREFIXs = PREFIXs_list.get("f");
                    String prefix = oval_seq.substring(oval_size);
                    boolean has_trans_edge = false;
                    for(int j = 0; stored_PREFIXs != null && j < stored_PREFIXs.size(); j++){
                        if (stored_PREFIXs.get(j).oval_size == oval_size && stored_PREFIXs.get(j).str.length() == oval_seq.length()) {
                            continue;
                        }
                        if (ERRORRATE <= 0) {
                            if (prefix.startsWith(stored_PREFIXs.get(j).suffix)){
                                //System.err.println( "remove:" +  node.getNodeId() + " " + oval_type + " " + oval_id);
                                //output.collect(new Text("prefix_f"),  new Text(stored_PREFIXs.get(j).id + " " + stored_PREFIXs.get(j).edge_type ));
                                //output.collect(new Text("remove_f"),  new Text(node.getNodeId() + " " + oval_type + " " + oval_id));
                                reporter.incrCounter("Brush", "trans_edge", 1);
                                has_trans_edge = true;
                                node.addRemovalEdge(oval_id, Node.flip_link(oval_type), node.getNodeId(), oval_size);
                                break;
                            }
                        } else {
                           /* String previous = stored_PREFIXs.get(j).str.substring(stored_PREFIXs.get(j).oval_size-oval_size);
                            String current;
                            if (oval_seq.length() >= oval_size + (stored_PREFIXs.get(j).str.length()-stored_PREFIXs.get(j).oval_size )) {
                                current = oval_seq.substring(0, oval_size + (stored_PREFIXs.get(j).str.length()-stored_PREFIXs.get(j).oval_size));
                            } else {
                                current = oval_seq;
                            }
                            boolean prefix_match = false;
                            for(int k=0; k <= current.length() - K; k++) {
                                if (previous.substring(k, K+k).equals(current.substring(k, K+k))) {
                                    prefix_match = true;
                                    break;
                                }
                            }
                            if (prefix_match) {
                                int distance = fastdistance(previous, current);
                                float error_rate = (float)distance / (float)current.length();
                                if ( error_rate <= ERRORRATE ) {
                                    reporter.incrCounter("Brush", "trans_edge", 1);
                                    has_trans_edge = true;
                                    node.addRemovalEdge(oval_id, Node.flip_link(oval_type), node.getNodeId(), oval_size);
                                    break;
                                }
                            }*/
                            //output.collect(new Text(previous), new Text(current + " " + error_rate + " " + ERRORRATE + " :" + distance + " |" + current.length()));
                        }
                    }
                    if (has_trans_edge) {
                        continue;
                    }

                    //output.collect(new Text(node.getNodeId()), new Text("O" + "\t" + olist.get(i).id));
                    //\\\\\\\\\\\\\ Store confirmed edge
                    if (PREFIXs_list.containsKey("f")) {
                        PREFIXs_list.get("f").add(new Prefix(oval_id,oval_type,oval_seq, oval_size));
                    } else {
                        List<Prefix> tmp_PREFIXs = new ArrayList<Prefix>();
                        tmp_PREFIXs.add(new Prefix(oval_id,oval_type,oval_seq, oval_size));
                        PREFIXs_list.put("f", tmp_PREFIXs);
                    }
                    if (edges_list.containsKey(oval_type))
                    {
                        edges_list.get(oval_type).add(edge_content);
                        IDs_flist.get(oval_type).add(oval_id);
                    }
                    else
                    {
                        List<String> tmp_edges = null;
                        tmp_edges = new ArrayList<String>();
                        tmp_edges.add(edge_content);
                        edges_list.put(oval_type, tmp_edges);
                        List<String> tmp_IDs = new ArrayList<String>();
                        tmp_IDs.add(oval_id);
                        IDs_flist.put(oval_type, tmp_IDs);
                    }
                }
            }
            //\\\\\\\\\\\\\\\\\\\\\\\\\\ r_overlap
            int r_choices = o_rlist.size();
            if (r_choices > 0)
			{
                // Sort overlap strings in order of decreasing overlap size
                Collections.sort(o_rlist, new OvelapSizeComparator_r());
                //output.collect(new Text(node.getNodeId()), new Text(olist.size() + "!"));
                // See if there are any pairwise compatible strings
                for (int i = 0; i < r_choices; i++)
                {
                    String oval_id = o_rlist.get(i).id;
                    String oval_type = o_rlist.get(i).edge_type;
                    //String node_dir = oval_type.substring(0, 1);
                    String oval_dir = oval_type.substring(1);
                    int oval_size = o_rlist.get(i).overlap_size;
                    String edge_content = oval_id + "!" + oval_size;

                    String oval_seq_tmp = Node.dna2str(o_rlist.get(i).str);
                    String oval_seq;
                    if (oval_dir.equals("r")){
                        oval_seq = Node.rc(oval_seq_tmp);
                    } else {
                        oval_seq = oval_seq_tmp;
                    }

                    //output.collect(new Text(node.getNodeId()), new Text("A" + "\t" + olist.get(i).id + "\t" + node.toNodeMsg()));
                    //\\ Self contained filter
                    if (oval_size == oval_seq.length() && oval_size == node.str().length()) {
                        //node.addRemovalEdge(oval_id, Node.flip_link(oval_type), node.getNodeId(), oval_size);
                        reporter.incrCounter("Brush", "contained_edge", 1);
                        //continue;
                    }
                    //\\\\\\\\\\\ Maximal Overlap filter
                    List<String> stored_IDs = IDs_rlist.get(oval_type);
                    boolean has_large_overlap = false;
                    if (stored_IDs != null && stored_IDs.contains(oval_id))
                    {
                        has_large_overlap = true;
                    }
                    if (has_large_overlap){
                        node.addRemovalEdge(oval_id, Node.flip_link(oval_type), node.getNodeId(), oval_size);
                        continue;
                    }

                   //\\\\\\\\\\\ Transitive Reduction filter
                    List<Prefix> stored_PREFIXs = PREFIXs_list.get("r");
                    String prefix = oval_seq.substring(oval_size);
                    boolean has_trans_edge = false;
                    for(int j = 0; stored_PREFIXs != null && j < stored_PREFIXs.size(); j++){
                        if (stored_PREFIXs.get(j).oval_size == oval_size && stored_PREFIXs.get(j).str.length() == oval_seq.length()) {
                            continue;
                        }
                        if (ERRORRATE <= 0) {
                            if (prefix.startsWith(stored_PREFIXs.get(j).suffix)){
                                //System.err.println( "remove:" +  node.getNodeId() + " " + oval_type + " " + oval_id);
                                //output.collect(new Text("prefix_r"),  new Text(stored_PREFIXs.get(j).id + " " + stored_PREFIXs.get(j).edge_type ));
                                //output.collect(new Text("remove_r"),  new Text(node.getNodeId() + " " + oval_type + " " + oval_id));
                                reporter.incrCounter("Brush", "trans_edge", 1);
                                has_trans_edge = true;
                                node.addRemovalEdge(oval_id, Node.flip_link(oval_type), node.getNodeId(), oval_size);
                                break;
                            }
                        } else {
                          /*  String previous = stored_PREFIXs.get(j).str.substring(stored_PREFIXs.get(j).oval_size-oval_size);
                            String current;
                            if (oval_seq.length() >= oval_size + (stored_PREFIXs.get(j).str.length()-stored_PREFIXs.get(j).oval_size )) {
                                current = oval_seq.substring(0, oval_size + (stored_PREFIXs.get(j).str.length()-stored_PREFIXs.get(j).oval_size));
                            } else {
                                current = oval_seq;
                            }
                            boolean prefix_match = false;
                            for(int k=0; k <= current.length() - K; k++) {
                                if (previous.substring(k, K+k).equals(current.substring(k, K+k))) {
                                    prefix_match = true;
                                    break;
                                }
                            }
                            if (prefix_match) {
                                int distance = fastdistance(previous, current);
                                float error_rate = (float)distance / (float)current.length();
                                if ( error_rate <= ERRORRATE ) {
                                    reporter.incrCounter("Brush", "trans_edge", 1);
                                    has_trans_edge = true;
                                    node.addRemovalEdge(oval_id, Node.flip_link(oval_type), node.getNodeId(), oval_size);
                                    break;
                                }
                            }*/
                            //output.collect(new Text(previous), new Text(current + " " + error_rate + " " + ERRORRATE + " :" + distance + " |" + current.length()));
                        }
                    }
                    if (has_trans_edge) {
                        continue;
                    }

                    //output.collect(new Text(node.getNodeId()), new Text("O" + "\t" + olist.get(i).id));
                    //\\\\\\\\\\\\\ Store confirmed edge
                    if (PREFIXs_list.containsKey("r")) {
                        PREFIXs_list.get("r").add(new Prefix(oval_id,oval_type,oval_seq, oval_size));
                    } else {
                        List<Prefix> tmp_PREFIXs = new ArrayList<Prefix>();
                        tmp_PREFIXs.add(new Prefix(oval_id,oval_type,oval_seq, oval_size));
                        PREFIXs_list.put("r", tmp_PREFIXs);
                    }
                    if (edges_list.containsKey(oval_type))
                    {
                        edges_list.get(oval_type).add(edge_content);
                        IDs_rlist.get(oval_type).add(oval_id);
                    }
                    else
                    {
                        List<String> tmp_edges = null;
                        tmp_edges = new ArrayList<String>();
                        tmp_edges.add(edge_content);
                        edges_list.put(oval_type, tmp_edges);
                        List<String> tmp_IDs = new ArrayList<String>();
                        tmp_IDs.add(oval_id);
                        IDs_rlist.put(oval_type, tmp_IDs);
                    }
                }
            }

            //\\\\\\\\\\\\\\\\\ set Edges
            for(String con : Node.edgetypes)
            {
                node.clearEdges(con);
                List<String> edges = edges_list.get(con);
                if (edges != null)
                {
                    node.setEdges(con, edges);
                }
            }
            //if (!node.hasCustom("contained")){
                output.collect(new Text(node.getNodeId()), new Text(node.toNodeMsg()));
            //}

		}
    }


	public RunningJob run(String inputPath, String outputPath) throws Exception
	{
		sLogger.info("Tool name: TransitiveReduction");
		sLogger.info(" - input: "  + inputPath);
		sLogger.info(" - output: " + outputPath);

		//JobConf conf = new JobConf(Stats.class);
        JobConf conf = new JobConf(TransitiveReduction.class);
		conf.setJobName("TransitiveReduction " + inputPath);

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

		conf.setMapperClass(TransitiveReductionMapper.class);
		conf.setReducerClass(TransitiveReductionReducer.class);

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
		int res = ToolRunner.run(new Configuration(), new TransitiveReduction(), args);
		System.exit(res);
	}
}
