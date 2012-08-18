/*
    GenReverseEdge.java
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


public class GenReverseEdge extends Configured implements Tool
{
	private static final Logger sLogger = Logger.getLogger(GenReverseEdge.class);

    // GenReverseEdgeMapper
	///////////////////////////////////////////////////////////////////////////

	public static class GenReverseEdgeMapper extends MapReduceBase
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


    // GenReverseEdgeReducer
	///////////////////////////////////////////////////////////////////////////

	public static class GenReverseEdgeReducer extends MapReduceBase
	implements Reducer<Text, Text, Text, Text>
	{
        private static int K = 0;

		public void configure(JobConf job) {
			K = Integer.parseInt(job.get("K"));
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
            //\\ one node
			if (sawnode != 1)
			{
				throw new IOException("ERROR: Didn't see exactly 1 nodemsg (" + sawnode + ") for " + nodeid.toString());
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
                    String edge_content = oval_id + "!" + oval_size;
                    //\\\\\\\\\\\ filter by already existing edge
                    List<String> exist_edge = node.getEdges(oval_type);
                    if (exist_edge != null && exist_edge.contains(edge_content)){
                        continue;
                    }

                    //\\\\\\\\\\\\\ Store confirmed edge
                    if (edges_list.containsKey(oval_type))
                    {
                        edges_list.get(oval_type).add(edge_content);
                    }
                    else
                    {
                        List<String> tmp_edges = null;
                        tmp_edges = new ArrayList<String>();
                        tmp_edges.add(edge_content);
                        edges_list.put(oval_type, tmp_edges);
                    }
                }
            }
            //\\\\\\\\\\\\\\\\\ set Edges
            for(String con : Node.edgetypes)
            {
                //node.clearEdges(con);
                List<String> edges = edges_list.get(con);
                if (edges != null)
                {
                    //node.setEdges(con, edges);
                    for(int i=0; i < edges.size(); i++) {
                        node.addEdge(con, edges.get(i));
                    }
                }
            }
            output.collect(new Text(node.getNodeId()), new Text(node.toNodeMsg()));

		}
    }


	public RunningJob run(String inputPath, String outputPath) throws Exception
	{
		sLogger.info("Tool name: GenReverseEdge");
		sLogger.info(" - input: "  + inputPath);
		sLogger.info(" - output: " + outputPath);

		//JobConf conf = new JobConf(Stats.class);
        JobConf conf = new JobConf(GenReverseEdge.class);
		conf.setJobName("GenReverseEdge " + inputPath);

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

		conf.setMapperClass(GenReverseEdgeMapper.class);
		conf.setReducerClass(GenReverseEdgeReducer.class);

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
		int res = ToolRunner.run(new Configuration(), new GenReverseEdge(), args);
		System.exit(res);
	}
}


