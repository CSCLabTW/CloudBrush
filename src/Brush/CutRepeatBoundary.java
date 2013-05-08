/*
    CutRepreatBoundary.java
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


public class CutRepeatBoundary extends Configured implements Tool
{
	private static final Logger sLogger = Logger.getLogger(CutRepeatBoundary.class);

	// CutRepeatBoundaryMapper
	///////////////////////////////////////////////////////////////////////////

	public static class CutRepeatBoundaryMapper extends MapReduceBase
    implements Mapper<LongWritable, Text, Text, Text>
	{
		private static int K = 0;
       
        static public float KMERCOV = 30f;
        static public float READLEN = 36;
        /*public static int MAXBUBBLELEN = 0;*/

		public void configure(JobConf job)
		{
			K = Integer.parseInt(job.get("K"));
            KMERCOV = Float.parseFloat(job.get("KMERCOV"));
            READLEN = (float)Long.parseLong(job.get("READLENGTH"));
			/*MAXBUBBLELEN = Integer.parseInt(job.get("MAXBUBBLELEN"));*/
		}

		public void map(LongWritable lineid, Text nodetxt,
				OutputCollector<Text, Text> output, Reporter reporter)
		throws IOException
		{
			Node node = new Node();
			node.fromNodeMsg(nodetxt.toString());
            /*int fdegree = node.degree("f");
			int rdegree = node.degree("r");
            int tips = 0;
            if (fdegree + rdegree <= 1) {
                tips = 1;
            }*/
            int unique = 0;
            //\\define unique
            float A_statistic = 0;
            A_statistic = node.str().length()*KMERCOV/(READLEN-K+1)-(float)((node.str().length()*node.cov()/READLEN)*Math.log(2));
            if (A_statistic > 10 ) {
               unique = 1;
            } else {
               unique = 0;
            }
            //if (node.isUnique()) {
                for(String key : Node.edgetypes ) {
                    List<String> edges = node.getEdges(key);
                    if (edges != null)
                    {
                        for(int i = 0; i < edges.size(); i++){
                            String [] vals = edges.get(i).split("!");
                            String edge_id = vals[0];
                            String oval_size = vals[1];
                            String con = Node.flip_link(key);
                            output.collect(new Text(edge_id), new Text(Node.DARKMSG + "\t" + con + "\t" + node.getNodeId() + "\t" + node.str_raw() + "\t" + oval_size + "\t" + node.cov() +"\t" + unique));
                        }
                    }
                }
            //}

			output.collect(new Text(node.getNodeId()), new Text(node.toNodeMsg()));
			reporter.incrCounter("Brush", "nodes", 1);

		}
	}


	// CutRepeatBoundaryReducer
	///////////////////////////////////////////////////////////////////////////

	public static class CutRepeatBoundaryReducer extends MapReduceBase
	implements Reducer<Text, Text, Text, Text>
	{
		private static int K = 0;
        private static int READLEN = 0;
        private static float majority = 0.6f;
        private static float PWM_N = 0.1f;
        static public long READS = 0;
        static public long CTG_SUM = 0;
        static public float KMERCOV = 30f;
        
      

		public void configure(JobConf job) {
			K = Integer.parseInt(job.get("K"));
            READLEN = (int)Long.parseLong(job.get("READLENGTH"));
            majority = Float.parseFloat(job.get("MAJORITY"));
            PWM_N = Float.parseFloat(job.get("PWM_N"));
            KMERCOV = Float.parseFloat(job.get("KMERCOV"));
            //READS = Long.parseLong(job.get("READS"));
            //CTG_SUM = Long.parseLong(job.get("CTG_SUM"));
		}

         public class EdgeInfo
		{
			public String id;
            public String str;
            public String edge_type;
			public int overlap_size;
            public float cov;
            public int unique;

			// for gray edge
            public EdgeInfo(String[] vals, int offset) throws IOException
			{
				id = vals[offset+1];
                edge_type = vals[offset+0];
                str = vals[offset+2];
                overlap_size = Integer.parseInt(vals[offset+3]);
                cov = Float.parseFloat(vals[offset+4]);
                unique = Integer.parseInt(vals[offset+5]);
                // for black edge's end tag
			}

            // for black edge
            public EdgeInfo(String con1, String node_id1, String str_raw1, int oval_size1, float cov1, int tips1){
                id = node_id1;
                edge_type = con1;
                str = str_raw1;
                overlap_size = oval_size1;
                cov = cov1;
                unique = tips1;
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
                    EdgeInfo ei = new EdgeInfo(vals[1], vals[2], vals[3], Integer.parseInt(vals[4]), Float.parseFloat(vals[5]), Integer.parseInt(vals[6]));
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

            //\\define unique
            double A_statistic = 0;
            A_statistic = node.str().length()*KMERCOV/(READLEN-K+1)-(float)((node.str().length()*node.cov()/READLEN)*Math.log(2));            
            //A_statistic = node.len() * ((float)READS/(float)CTG_SUM) - node.getPairEnds().size()*Math.log(2);
            if (A_statistic > 10 ) {
                node.setisUnique(true);
                reporter.incrCounter("Brush", "unique", 1);
            } else {
                node.setisUnique(false);
                reporter.incrCounter("Brush", "repeat", 1);
            }
            //\\\
            EdgeInfo edge = null;
   
            int Consensus_threshold = (READLEN - K)/2;
            if (flist.size() > 1) {
                if (node.isUnique()) {
                //\\\\\\\\\
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
                    f_consensus = Node.Consensus(f_edge_list, majority, PWM_N);
                    //f_consensus = Node.Consensus2(f_edge_list, PWM_N);
                    if (f_consensus != null ) { 
                        for(int i = 0; i < flist.size(); i++) {
                            edge= flist.get(i);
                            String str = Node.dna2str(edge.str);
                            if (edge.edge_type.charAt(1) == 'r'){
                                str = Node.rc(str);
                            }
                            String str_cut = "";
                            str_cut = str.substring(edge.overlap_size);
                            int count_match = 0;
                            int miss_match = 0;
                            int compare_len = 0;
                            for(int j=0; j < str_cut.length() && j < f_consensus.length(); j++ ) {
                                if (str_cut.charAt(j) == f_consensus.charAt(j)) {
                                    count_match = count_match + 1;
                                } else {
                                    if (!(f_consensus.charAt(j) == 'N')) {
                                        miss_match = miss_match + 1;
                                    }     
                                }
                                compare_len = j;
                            }
                            if (miss_match <=0 /*|| edge.unique == 0*/) {
                                // keep edge
                            } else {
                                node.addRemovalEdge(node.getNodeId(), edge.edge_type, edge.id, edge.overlap_size);
                                node.addRemovalEdge(edge.id, Node.flip_link(edge.edge_type), node.getNodeId(), edge.overlap_size);
                                reporter.incrCounter("Brush", "edge_removal", 1); 
                            }
                        }
                    }
                //\\\\
                } // node is unique
                //\\\\\\\\\\\\\\\\\\\\\\\\\\\
                // count overlap boundary
                int current_overlap = 0;
                float overlap_count = 0;
                float total_cov = 0;
                int boundary_overlap = 0;
                boolean cut_boundary = false;
                for (int i = 0; i < flist.size(); i++) {
                    total_cov = total_cov + flist.get(i).cov;
                    if (flist.get(i).id.equals(node.getNodeId())) {
                        edge= flist.get(i);
                        node.addRemovalEdge(node.getNodeId(), edge.edge_type, edge.id, edge.overlap_size);
                        node.addRemovalEdge(edge.id, Node.flip_link(edge.edge_type), node.getNodeId(), edge.overlap_size);
                        reporter.incrCounter("Brush", "edge_removal", 1);     
                    }
                }
                for(int i = 0; i < flist.size(); i++) {
                    if (flist.get(i).overlap_size == current_overlap) {
                        overlap_count = overlap_count + flist.get(i).cov;
                        if (overlap_count > 1) {
                            boundary_overlap = flist.get(i).overlap_size;
                            break;
                        }
                    } else {
                        //overlap_count = flist.get(i).cov;                       
                        overlap_count = 1;
                        current_overlap = flist.get(i).overlap_size;
                    }
                }
                if (boundary_overlap != 0 && boundary_overlap < flist.get(0).overlap_size){
                    cut_boundary = true;
                }
                if (cut_boundary) { 
                    for(int i = 0; i < flist.size(); i++) {
                        edge= flist.get(i);
                        if (edge.overlap_size <= boundary_overlap) {
                            node.addRemovalEdge(node.getNodeId(), edge.edge_type, edge.id, edge.overlap_size);
                            node.addRemovalEdge(edge.id, Node.flip_link(edge.edge_type), node.getNodeId(), edge.overlap_size);
                            reporter.incrCounter("Brush", "edge_removal", 1); 
                        }
                    }
                }
            }
            if (rlist.size() > 1) {
                if (node.isUnique()) {
                //\\\\\\\
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
                    r_consensus = Node.Consensus(r_edge_list, majority, PWM_N);
                    //r_consensus = Node.Consensus2(r_edge_list, PWM_N);
                    //\\\\\\\\\\\\\\\\\\\\\\\\\\\
                    if (r_consensus != null ) { 
                        for(int i = 0; i < rlist.size(); i++) {
                            edge= rlist.get(i);
                            String str = Node.dna2str(edge.str);
                            if (edge.edge_type.charAt(1) == 'r'){
                                str = Node.rc(str);
                            }
                            String str_cut = "";
                            str_cut = str.substring(edge.overlap_size);
                            int count_match = 0;
                            int miss_match = 0;
                            int compare_len = 0;
                            for(int j=0; j < str_cut.length() && j < r_consensus.length(); j++ ) {
                                if (str_cut.charAt(j) == r_consensus.charAt(j)) {
                                    count_match = count_match + 1;
                                } else {
                                     if (!(r_consensus.charAt(j) == 'N')) {
                                        miss_match = miss_match + 1;
                                    }     
                                }
                                compare_len = j;
                            }
                            if (miss_match <=0 /*|| edge.unique == 0*/) {
                                // keep edge
                            } else {
                                node.addRemovalEdge(node.getNodeId(), edge.edge_type, edge.id, edge.overlap_size);
                                node.addRemovalEdge(edge.id, Node.flip_link(edge.edge_type), node.getNodeId(), edge.overlap_size);
                                reporter.incrCounter("Brush", "edge_removal", 1); 
                            }
                        }
                    }
                //\\\\\
                } // node is unique
                // count overlap boundary
                int current_overlap = 0;
                float overlap_count = 0;
                float total_cov = 0;
                int boundary_overlap = 0;
                boolean cut_boundary = false;
                for (int i = 0; i < rlist.size(); i++) {
                    total_cov = total_cov + rlist.get(i).cov;
                    if (rlist.get(i).id.equals(node.getNodeId())) {
                        edge= rlist.get(i);
                        node.addRemovalEdge(node.getNodeId(), edge.edge_type, edge.id, edge.overlap_size);
                        node.addRemovalEdge(edge.id, Node.flip_link(edge.edge_type), node.getNodeId(), edge.overlap_size);
                        reporter.incrCounter("Brush", "edge_removal", 1);     
                    }
                }
                for(int i = 0; i < rlist.size(); i++) {
                    if (rlist.get(i).overlap_size == current_overlap) {
                        overlap_count = overlap_count + rlist.get(i).cov;
                        if (overlap_count > 1) {
                            boundary_overlap = rlist.get(i).overlap_size;
                            break;
                        }
                    } else {
                        //overlap_count = rlist.get(i).cov;
                        overlap_count = 1;
                        current_overlap = rlist.get(i).overlap_size;
                    }
                }
                if (boundary_overlap != 0 && boundary_overlap < rlist.get(0).overlap_size){
                    cut_boundary = true;
                }
                if (cut_boundary) { 
                    for(int i = 0; i < rlist.size(); i++) {
                        edge= rlist.get(i);
                        if (edge.overlap_size <= boundary_overlap) {
                            node.addRemovalEdge(node.getNodeId(), edge.edge_type, edge.id, edge.overlap_size);
                            node.addRemovalEdge(edge.id, Node.flip_link(edge.edge_type), node.getNodeId(), edge.overlap_size);
                            reporter.incrCounter("Brush", "edge_removal", 1); 
                        }
                    }
                }
            }
            output.collect(nodeid, new Text(node.toNodeMsg()));
		}
	}




	// Run Tool
	///////////////////////////////////////////////////////////////////////////

	public RunningJob run(String inputPath, String outputPath, long reads, long ctg_sum) throws Exception
	{
		sLogger.info("Tool name: CutRepeatBoundary");
		sLogger.info(" - input: "  + inputPath);
		sLogger.info(" - output: " + outputPath);

		JobConf conf = new JobConf(CutRepeatBoundary.class);
		conf.setJobName("CutRepeatBoundary " + inputPath + " " + BrushConfig.K);
        //conf.setFloat("Error_Rate", ErrorRate);
        //conf.setFloat("Exp_Cov", Exp_Cov);
        conf.setLong("READS", reads);
        conf.setLong("CTG_SUM", ctg_sum);

		BrushConfig.initializeConfiguration(conf);

		FileInputFormat.addInputPath(conf, new Path(inputPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(CutRepeatBoundaryMapper.class);
		conf.setReducerClass(CutRepeatBoundaryReducer.class);

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

		run(inputPath, outputPath, 0, 0);
		return 0;
	}


	// Main
	///////////////////////////////////////////////////////////////////////////

	public static void main(String[] args) throws Exception
	{
		int res = ToolRunner.run(new Configuration(), new CutRepeatBoundary(), args);
		System.exit(res);
	}
}
