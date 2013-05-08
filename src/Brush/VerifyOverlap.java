/*
    VerifyOverlap.java
    2012 Ⓒ CloudBrush, developed by Chien-Chih Chen (rocky@iis.sinica.edu.tw), 
    released under Apache License 2.0 (http://www.apache.org/licenses/LICENSE-2.0) 
    at: https://github.com/ice91/CloudBrush
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


public class VerifyOverlap extends Configured implements Tool
{
	private static final Logger sLogger = Logger.getLogger(VerifyOverlap.class);

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

    // VerifyOverlapMapper
	///////////////////////////////////////////////////////////////////////////

	public static class VerifyOverlapMapper extends MapReduceBase
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
                /// modify ... 01/15
				//String key = "r" + adj;
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


    // VerifyOverlapReducer
	///////////////////////////////////////////////////////////////////////////

	public static class VerifyOverlapReducer extends MapReduceBase
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

        class OvelapSizeComparator implements Comparator {
            public int compare(Object element1, Object element2) {
                OverlapInfo obj1 = (OverlapInfo) element1;
                OverlapInfo obj2 = (OverlapInfo) element2;
                if ((int)(obj1.overlap_size - obj2.overlap_size) >= 0) {
                    return -1;
                } else {
                    return 1;
                }
            }
        }

        public class Prefix {
            public String id;
            public String str;
            public String edge_type;
            public Prefix(String id1, String edge_type1, String str1){
                id = id1;
                edge_type = edge_type1;
                str = str1;
            }
        }


		public void reduce(Text nodeid, Iterator<Text> iter,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException
		{
			Node node = new Node(nodeid.toString());
		    List<OverlapInfo> olist = new ArrayList<OverlapInfo>();
            
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
                    olist.add(oi);
                    //output.collect(new Text(node.getNodeId()), new Text("X" + "\t" + oi.id ));
				}
				else
				{
					throw new IOException("Unknown msgtype: " + msg);
				}
			}
            //\\ two brush
			if (sawnode != 1 && sawnode != 2)
			{
				throw new IOException("ERROR: Didn't see exactly 1 && 2 nodemsg (" + sawnode + ") for " + nodeid.toString());
			}

            //\\ store confirmed edges
            Map<String, List<String>> edges_list = new HashMap<String, List<String>>();
            Map<String, List<String>> IDs_list = new HashMap<String, List<String>>();
            Map<String, List<Prefix>> PREFIXs_list = new HashMap<String, List<Prefix>>();

            int choices = olist.size();
            //output.collect(new Text(node.getNodeId()), new Text(choices + ""));
			if (choices > 0)
			{
                // Sort overlap strings in order of decreasing overlap size
                Collections.sort(olist, new OvelapSizeComparator());
                //output.collect(new Text(node.getNodeId()), new Text(olist.size() + "!"));
                // See if there are any pairwise compatible strings
                for (int i = 0; i < choices; i++)
                {
                    String oval_id = olist.get(i).id;
                    String oval_type = olist.get(i).edge_type;
                    String node_dir = oval_type.substring(0, 1);
                    String oval_dir = oval_type.substring(1);
                    int oval_size = olist.get(i).overlap_size;
                    String oval_seq_tmp = Node.dna2str(olist.get(i).str);
                    String oval_seq;
                    if (oval_dir.equals("r")){
                        oval_seq = Node.rc(oval_seq_tmp);
                    } else {
                        oval_seq = oval_seq_tmp;
                    }
                    
                    //output.collect(new Text(node.getNodeId()), new Text("A" + "\t" + olist.get(i).id + "\t" + node.toNodeMsg()));
                    //\\\\\\\\\\\ contained reads filter for ovrtlap


                    //\\\\\\\\\\\ Maximal Overlap filter
                    List<String> stored_IDs = IDs_list.get(oval_type);
                    boolean has_large_overlap = false;
                    if (stored_IDs != null && stored_IDs.contains(olist.get(i).id))
                    {
                        has_large_overlap = true;
                    }
                    if (has_large_overlap){
                        continue;
                    }

                    //output.collect(new Text(node.getNodeId()), new Text("B" + "\t" + olist.get(i).id));
                    //\\\\\\\\\\\ Alignment filter
                    String node_seq;
                    if (node_dir.equals("r")){
                        node_seq = Node.rc(node.str());
                    } else {
                        node_seq = node.str();
                    }
                    String str1;
                    String str2;
                    str1 = node_seq.substring(node_seq.length()-oval_size, node_seq.length());
                    if (oval_size > oval_seq.length()) {
                        //contained reads
                        continue;
                        //throw new IOException("ERROR: oval_seq : " + oval_seq + " node_seq: " + node_seq + " oval_size: " + oval_size + " node_id:" + nodeid.toString() + " oval_id:" + oval_id);
                    } else {
                        str2 = oval_seq.substring(0, oval_size);
                    }
                
                 //   str1 = node_seq.substring(node_seq.length() - oval_size + K);
                 //   str2 = oval_seq.substring(K, oval_size);

                    if (!str1.equals(str2)){
                        continue;
                    }

                    //output.collect(new Text(node.getNodeId()), new Text("O" + "\t" + olist.get(i).id + "\t" + str1 +"\t" + str2 + "\t" + distance + "\t" + error_rate));
                    //\\\\\\\\\\\\\ Store confirmed edge
                    String edge_content = oval_id + "!" + oval_size;
                    if (edges_list.containsKey(oval_type))
                    {
                        edges_list.get(oval_type).add(edge_content);
                        IDs_list.get(oval_type).add(oval_id);
                        //PREFIXs_list.get(node_dir).add(new Prefix(oval_id,oval_type,prefix));
                    }
                    else
                    {
                        List<String> tmp_edges = null;
                        tmp_edges = new ArrayList<String>();
                        tmp_edges.add(edge_content);
                        edges_list.put(oval_type, tmp_edges);
                        List<String> tmp_IDs = new ArrayList<String>();
                        tmp_IDs.add(olist.get(i).id);
                        IDs_list.put(oval_type, tmp_IDs);
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
            output.collect(new Text(node.getNodeId()), new Text(node.toNodeMsg()));
            
		}
    }


	public RunningJob run(String inputPath, String outputPath) throws Exception
	{
		sLogger.info("Tool name: VerifyOverlap");
		sLogger.info(" - input: "  + inputPath);
		sLogger.info(" - output: " + outputPath);

		//JobConf conf = new JobConf(Stats.class);
        JobConf conf = new JobConf(VerifyOverlap.class);
		conf.setJobName("VerifyOverlap " + inputPath);

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
        //conf.setClass("mapred.output.compression.codec", GzipCodec.class,CompressionCodec.class);

		conf.setMapperClass(VerifyOverlapMapper.class);
		conf.setReducerClass(VerifyOverlapReducer.class);

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
		int res = ToolRunner.run(new Configuration(), new VerifyOverlap(), args);
		System.exit(res);
	}
}

