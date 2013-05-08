/*
    DefineConsensus.java
    2012 Ⓒ CloudBrush, developed by Chien-Chih Chen (rocky@iis.sinica.edu.tw), 
    released under Apache License 2.0 (http://www.apache.org/licenses/LICENSE-2.0) 
    at: https://github.com/ice91/CloudBrush
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
import java.util.Collections;
import java.util.Comparator;

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


public class DefineConsensus extends Configured implements Tool
{
	private static final Logger sLogger = Logger.getLogger(DefineConsensus.class);

	// DefineConsensusMapper
	///////////////////////////////////////////////////////////////////////////

	public static class DefineConsensusMapper extends MapReduceBase
    implements Mapper<LongWritable, Text, Text, Text>
	{
		private static int K = 0;
       
		/*public static int MAXBUBBLELEN = 0;*/

		public void configure(JobConf job)
		{
			K = Integer.parseInt(job.get("K"));
			/*MAXBUBBLELEN = Integer.parseInt(job.get("MAXBUBBLELEN"));*/
		}

		public void map(LongWritable lineid, Text nodetxt,
				OutputCollector<Text, Text> output, Reporter reporter)
		throws IOException
		{
			Node node = new Node();
			node.fromNodeMsg(nodetxt.toString());
            int fdegree = node.degree("f");
			int rdegree = node.degree("r");
            int tips = 0;
          
            for(String key : Node.edgetypes ) {
                List<String> edges = node.getEdges(key);
                if (edges != null)
                {
                    for(int i = 0; i < edges.size(); i++){
                        String [] vals = edges.get(i).split("!");
                        String edge_id = vals[0];
                        String oval_size = vals[1];
                        String con = Node.flip_link(key);
                        reporter.incrCounter("Brush", "edges", 1);
                        output.collect(new Text(edge_id), new Text(Node.DARKMSG + "\t" + con + "\t" + node.getNodeId() + "\t" + node.str_raw() + "\t" + oval_size + "\t" + node.cov() + "\t" + tips));
                    }
                }
            }
           

			output.collect(new Text(node.getNodeId()), new Text(node.toNodeMsg()));
			reporter.incrCounter("Brush", "nodes", 1);

		}
	}


	// DefineConsensusReducer
	///////////////////////////////////////////////////////////////////////////

	public static class DefineConsensusReducer extends MapReduceBase
	implements Reducer<Text, Text, Text, Text>
	{
		private static int K = 0;
        private static int READLEN = 0;
        private static float majority = 0.75f;
        private static float PWM_N = 0.1f;
       

		public void configure(JobConf job) {
			K = Integer.parseInt(job.get("K"));
            READLEN = (int)Long.parseLong(job.get("READLENGTH"));
          
            majority = Float.parseFloat(job.get("MAJORITY"));
            PWM_N = Float.parseFloat(job.get("PWM_N"));
		}

         public class EdgeInfo
		{
			public String id;
            public String str;
            public String edge_type;
			public int overlap_size;
            public float cov;
           // public int tips;

			// for gray edge
            public EdgeInfo(String[] vals, int offset) throws IOException
			{
				id = vals[offset+1];
                edge_type = vals[offset+0];
                str = vals[offset+2];
                overlap_size = Integer.parseInt(vals[offset+3]);
                cov = Float.parseFloat(vals[offset+4]);
              //  tips = Integer.parseInt(vals[offset+5]);
                // for black edge's end tag
			}

            // for black edge
            public EdgeInfo(String con1, String node_id1, String str_raw1, int oval_size1, float cov1/*, int tips1*/){
                id = node_id1;
                edge_type = con1;
                str = str_raw1;
                overlap_size = oval_size1;
                cov = cov1;
                //tips = tips1;
            }

            public String toString()
			{
				return edge_type + " " + id + " " + overlap_size + " " + str + " " + cov;
			}
		}

         class EdgeComparator implements Comparator {
            public int compare(Object element1, Object element2) {
                EdgeInfo obj1 = (EdgeInfo) element1;
                EdgeInfo obj2 = (EdgeInfo) element2;
                //con + "|" + node_id + "|" + str_raw + "|" + oval_size + "|" + cov + "|" + end;
                if ((int) ( (Node.dna2str(obj1.str).length()-obj1.overlap_size) - (Node.dna2str(obj2.str).length()-obj2.overlap_size) ) >= 0) {
                    return -1;
                } else {
                    return 1;
                }
            }
        }

        class OvelapSizeComparator implements Comparator {
            public int compare(Object element1, Object element2) {
                EdgeInfo obj1 = (EdgeInfo) element1;
                EdgeInfo obj2 = (EdgeInfo) element2;
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

        class OvelapSizeComparator_f implements Comparator {
            public int compare(Object element1, Object element2) {
                EdgeInfo obj1 = (EdgeInfo) element1;
                EdgeInfo obj2 = (EdgeInfo) element2;
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
                EdgeInfo obj1 = (EdgeInfo) element1;
                EdgeInfo obj2 = (EdgeInfo) element2;
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

		public void reduce(Text nodeid, Iterator<Text> iter,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException
		{
			Node node = new Node(nodeid.toString());
            List<EdgeInfo> flist = new ArrayList<EdgeInfo>();
            List<EdgeInfo> rlist = new ArrayList<EdgeInfo>();

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
                else if (vals[0].equals(Node.DARKMSG))
				{
                    EdgeInfo ei = new EdgeInfo(vals[1], vals[2], vals[3], Integer.parseInt(vals[4]), Float.parseFloat(vals[5]));
                    //blist.add(ei);
                    if (ei.edge_type.charAt(0) == 'f') {
                        flist.add(ei);
                    } else if (ei.edge_type.charAt(0) == 'r') {
                        rlist.add(ei);
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

            //\\\
            EdgeInfo edge = null;
   
            int Consensus_threshold = (READLEN - K)/2;
            if (flist.size() > 1) {
                Collections.sort(flist, new OvelapSizeComparator_f());
                //\\PWM
                List<String> f_edge_list = new ArrayList<String>();
                EdgeInfo f_edge = null;
                String f_consensus = "";
                for(int i = 0; i < flist.size(); i++) {
                    f_edge = flist.get(i);
                    String str = Node.dna2str(f_edge.str);
                    String str_cut = "";
                    if (f_edge.edge_type.charAt(1) == 'r'){
                        str = Node.rc(str);
                    }
                    str_cut = str.substring(f_edge.overlap_size);
                    f_edge_list.add(str_cut+"!"+f_edge.cov);
                }
                //f_consensus = Node.Consensus(f_edge_list, majority, PWM_N);
                f_consensus = Node.Consensus2(f_edge_list, PWM_N);
                //\\\\\\\\\\\\\\\\\\\\\\\\\\\
                if (f_consensus != null ) { 
                    node.setColor(Node.Color.B, "f");
                } else {
                    node.setColor(Node.Color.W, "f");
                }
            } else {
                node.setColor(Node.Color.W, "f");
            }
            if (rlist.size() > 1) {
                Collections.sort(rlist, new OvelapSizeComparator_r());
                 //\\PWM
                List<String> r_edge_list = new ArrayList<String>();
                EdgeInfo r_edge = null;
                String r_consensus = "";
                for(int i = 0; i < rlist.size(); i++) {
                    r_edge = rlist.get(i);
                    String str = Node.dna2str(r_edge.str);
                    String str_cut = "";
                    if (r_edge.edge_type.charAt(1) == 'r'){
                        str = Node.rc(str);
                    }
                    str_cut = str.substring(r_edge.overlap_size);
                    r_edge_list.add(str_cut+"!"+r_edge.cov);
                }
                //r_consensus = Node.Consensus(r_edge_list, majority, PWM_N);
                r_consensus = Node.Consensus2(r_edge_list, PWM_N);
                //\\\\\\\\\\\\\\\\\\\\\\\\\\\
                if (r_consensus != null ) { 
                    node.setColor(Node.Color.B, "r");
                } else {
                    node.setColor(Node.Color.W, "r");
                }
            } else {
                node.setColor(Node.Color.W, "r");
            }
            output.collect(nodeid, new Text(node.toNodeMsg()));
		}
	}




	// Run Tool
	///////////////////////////////////////////////////////////////////////////

	public RunningJob run(String inputPath, String outputPath) throws Exception
	{
		sLogger.info("Tool name: DefineConsensus");
		sLogger.info(" - input: "  + inputPath);
		sLogger.info(" - output: " + outputPath);

		JobConf conf = new JobConf(DefineConsensus.class);
		conf.setJobName("DefineConsensus " + inputPath + " " + BrushConfig.K);
        //conf.setFloat("Error_Rate", ErrorRate);
        //conf.setFloat("Exp_Cov", Exp_Cov);

		BrushConfig.initializeConfiguration(conf);

		FileInputFormat.addInputPath(conf, new Path(inputPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(DefineConsensusMapper.class);
		conf.setReducerClass(DefineConsensusReducer.class);

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
		int res = ToolRunner.run(new Configuration(), new DefineConsensus(), args);
		System.exit(res);
	}
}

