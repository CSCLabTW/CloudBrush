/*
    AdjustMateEdge.java
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


public class AdjustMateEdge extends Configured implements Tool
{
	private static final Logger sLogger = Logger.getLogger(AdjustMateEdge.class);

    // AdjustMateEdgeMapper
	///////////////////////////////////////////////////////////////////////////

	public static class AdjustMateEdgeMapper extends MapReduceBase
    implements Mapper<LongWritable, Text, Text, Text>
	{
        private static int K = 0;
        static public float EXPCOV = 20f;
        static public float READLEN = 36;
        static public long READS = 0;
        static public long CTG_SUM = 0;
        
        public void configure(JobConf job) {
			K = Integer.parseInt(job.get("K"));
            EXPCOV = Float.parseFloat(job.get("EXPCOV"));
            READLEN = (float)Long.parseLong(job.get("READLENGTH"));
            READS = Long.parseLong(job.get("READS"));
            CTG_SUM = Long.parseLong(job.get("CTG_SUM"));
		}
		public void map(LongWritable lineid, Text nodetxt,
                OutputCollector<Text, Text> output, Reporter reporter)
                throws IOException
        {
			Node node = new Node();
			node.fromNodeMsg(nodetxt.toString());
            //\\define unique
            double A_statistic = 0;
            //A_statistic = node.str().length()*EXPCOV/(READLEN-K+1)-(float)((node.str().length()*node.cov()/READLEN)*Math.log(2));
            A_statistic = node.len() * ((float)READS/(float)CTG_SUM) - node.getPairEnds().size()*Math.log(2);
            if (A_statistic > 10 ) {
                node.setisUnique(true);
                reporter.incrCounter("Brush", "unique", 1);
            } else {
                node.setisUnique(false);
                reporter.incrCounter("Brush", "repeat", 1);
            }
			for (String key : Node.edgetypes)
			{
                List<String> edges = node.getEdges(key);
                if (edges != null)
                {
                    for(int i = 0; i < edges.size(); i++){
                        String [] vals = edges.get(i).split("!");
                        String edge_id = vals[0];
                        String oval_size = vals[1];
                        String con = Node.flip_link(key);
                        output.collect(new Text(edge_id), new Text(Node.DARKMSG + "\t" + con + "\t" + oval_size + "\t" + node.toNodeMsg(true)));
                    }
                }
			}
            output.collect(new Text(node.getNodeId()), new Text(node.toNodeMsg()));
			reporter.incrCounter("Brush", "nodes", 1);
            
        }
	}


    // AdjustMateEdgeReducer
	///////////////////////////////////////////////////////////////////////////

	public static class AdjustMateEdgeReducer extends MapReduceBase
	implements Reducer<Text, Text, Text, Text>
	{
        private static int K = 0;
        static public long INSLEN = 200;
        static public long INSLEN_SD = 20;
       
		public void configure(JobConf job) {
			K = Integer.parseInt(job.get("K")); 
            INSLEN = Long.parseLong(job.get("INSLENGTH"));
            INSLEN_SD = Long.parseLong(job.get("INSLENGTH_SD"));
		}

        class NeighborInfo
		{
			String type;
            String oval_size;
			Node   node;		
		}
        
		public void reduce(Text nodeid, Iterator<Text> iter,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException
		{
			Node node = new Node(nodeid.toString());
            List<NeighborInfo> flist = new ArrayList<NeighborInfo>();
            List<NeighborInfo> rlist = new ArrayList<NeighborInfo>();
          
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
                    NeighborInfo ni = new NeighborInfo();
					
					ni.type  = vals[1];
					ni.oval_size = vals[2];
                    ni.node = new Node(vals[3]);
					ni.node.parseNodeMsg(vals, 4);
                  
                    if (ni.type.charAt(0) == 'f') {
                        flist.add(ni);
                    } else if (ni.type.charAt(0) == 'r') {
                        rlist.add(ni);
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
            
            int pairs_support;
            if (node.isUnique()) {
            //\\\\\\\\\\\\ 
            boolean keep_edge = false;
            int keep_edge_idx = -1;
            int current_support = 0;
            for(int i = 0; i < flist.size(); i++) {
                pairs_support = 0;
                for(int j=0; j < rlist.size(); j++){
                    if (!flist.get(i).node.getNodeId().equals(rlist.get(j).node.getNodeId())) {
                        if ((flist.get(i).node.len() + rlist.get(j).node.len() + node.len() - Integer.parseInt(flist.get(i).oval_size) - Integer.parseInt(rlist.get(j).oval_size)) < (INSLEN + 2*INSLEN_SD)  ){
                            keep_edge = true; //edge distance small than insert size, do not use pair end information
                            break;
                            //continue;
                        }
                    }
                }
                if (keep_edge) {
                    break;
                }
                if (!flist.get(i).node.getNodeId().equals(node.getNodeId())) {
                    pairs_support = Node.Count_PairEnd(node.getPairEnds(), flist.get(i).node.getPairEnds());
                }
                int tmp;
                for(int j=0; j < rlist.size(); j++){
                    if (!flist.get(i).node.getNodeId().equals(rlist.get(j).node.getNodeId())) {
                        tmp  = Node.Count_PairEnd(flist.get(i).node.getPairEnds(), rlist.get(j).node.getPairEnds());
                        if (tmp > pairs_support) {
                            pairs_support = tmp;
                        }
                    }
                }
                
                if (pairs_support > current_support) {
                    keep_edge_idx = i;
                    current_support = pairs_support;
                }
                /*if (pairs_support < 1 ) {
                    node.addRemovalEdge(node.getNodeId(), flist.get(i).type, flist.get(i).node.getNodeId(), Integer.parseInt(flist.get(i).oval_size));
                    node.addRemovalEdge(flist.get(i).node.getNodeId(), Node.flip_link(flist.get(i).type), node.getNodeId(), Integer.parseInt(flist.get(i).oval_size));
                    reporter.incrCounter("Brush", "edge_removal", 1); 
                }
                if (pairs_support > 5 || !node.isUnique()) {
                    reporter.incrCounter("Brush", "5_edges", 1); 
                }
                if (pairs_support > 10 || !node.isUnique()) {
                    reporter.incrCounter("Brush", "10_edges", 1); 
                }
                if ((float)pairs_support/node.cov() > 0.5f || !node.isUnique()) {
                    reporter.incrCounter("Brush", "05_edges", 1); 
                }
                if ((float)pairs_support/node.cov() > 0.1f || !node.isUnique()) {
                    reporter.incrCounter("Brush", "01_edges", 1); 
                }
                reporter.incrCounter("Brush", "edges", 1); */
            }
            // remove f_edge
            // keep max pair support edge, remove other edge
            if (!keep_edge && current_support > 0) {
                for(int i=0; i < flist.size(); i++) {
                    if ( i != keep_edge_idx) {
                        node.addRemovalEdge(node.getNodeId(), flist.get(i).type, flist.get(i).node.getNodeId(), Integer.parseInt(flist.get(i).oval_size));
                        node.addRemovalEdge(flist.get(i).node.getNodeId(), Node.flip_link(flist.get(i).type), node.getNodeId(), Integer.parseInt(flist.get(i).oval_size));
                        reporter.incrCounter("Brush", "edge_removal", 1);
                    }
                }
            }
            
            keep_edge = false;
            keep_edge_idx = -1;
            current_support = 0;
            for(int i = 0; i < rlist.size(); i++) {
                pairs_support = 0;
                for(int j=0; j < flist.size(); j++){
                    if (!rlist.get(i).node.getNodeId().equals(flist.get(j).node.getNodeId())) {
                        if ((rlist.get(i).node.len() + flist.get(j).node.len() + node.len() - Integer.parseInt(rlist.get(i).oval_size) - Integer.parseInt(flist.get(j).oval_size)) < (INSLEN + 2*INSLEN_SD)  ){
                            keep_edge = true;
                            break;
                            //continue;
                        }
                    }
                }
                if (keep_edge) {
                    break;
                }
                if (!rlist.get(i).node.getNodeId().equals(node.getNodeId())) {
                    pairs_support = Node.Count_PairEnd(node.getPairEnds(), rlist.get(i).node.getPairEnds());
                }
                int tmp;
                for(int j=0; j < flist.size(); j++){
                    if (!rlist.get(i).node.getNodeId().equals(flist.get(j).node.getNodeId())) {
                        tmp  = Node.Count_PairEnd(rlist.get(i).node.getPairEnds(), flist.get(j).node.getPairEnds());
                        if (tmp > pairs_support) {
                            pairs_support = tmp;
                        }
                    }
                }
                if (pairs_support > current_support) {
                    keep_edge_idx = i;
                    current_support = pairs_support;
                }
                /*if (pairs_support < 1) {
                    node.addRemovalEdge(node.getNodeId(), rlist.get(i).type, rlist.get(i).node.getNodeId(), Integer.parseInt(rlist.get(i).oval_size));
                    node.addRemovalEdge(rlist.get(i).node.getNodeId(), Node.flip_link(rlist.get(i).type), node.getNodeId(), Integer.parseInt(rlist.get(i).oval_size));
                    reporter.incrCounter("Brush", "edge_removal", 1); 
                }
                if (pairs_support > 5 || !node.isUnique()) {
                    reporter.incrCounter("Brush", "5_edges", 1); 
                }
                if (pairs_support > 10 || !node.isUnique()) {
                    reporter.incrCounter("Brush", "10_edges", 1); 
                }
                if ((float)pairs_support/node.cov() > 0.5f || !node.isUnique() ) {
                    reporter.incrCounter("Brush", "05_edges", 1); 
                }
                if ((float)pairs_support/node.cov() > 0.1f || !node.isUnique()) {
                    reporter.incrCounter("Brush", "01_edges", 1); 
                }*/
                reporter.incrCounter("Brush", "edges", 1); 
            }
            // remove r_edge
            if (!keep_edge && current_support > 0) {
                for(int i=0; i < rlist.size(); i++) {
                    if ( i != keep_edge_idx) {
                        node.addRemovalEdge(node.getNodeId(), rlist.get(i).type, rlist.get(i).node.getNodeId(), Integer.parseInt(rlist.get(i).oval_size));
                        node.addRemovalEdge(rlist.get(i).node.getNodeId(), Node.flip_link(rlist.get(i).type), node.getNodeId(), Integer.parseInt(rlist.get(i).oval_size));
                        reporter.incrCounter("Brush", "edge_removal", 1); 
                    }
                }
            }
            //\\\\\\\\\\\\
            }  // node is unique
            output.collect(nodeid, new Text(node.toNodeMsg()));
            //output.collect(new Text(node.getNodeId()), new Text(choices + ""));
		}
    }


	public RunningJob run(String inputPath, String outputPath, long reads, long ctg_sum) throws Exception
	{
		sLogger.info("Tool name: AdjustMateEdge");
		sLogger.info(" - input: "  + inputPath);
		sLogger.info(" - output: " + outputPath);

		//JobConf conf = new JobConf(Stats.class);
        JobConf conf = new JobConf(AdjustMateEdge.class);
		conf.setJobName("AdjustMateEdge " + inputPath);

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

		conf.setMapperClass(AdjustMateEdgeMapper.class);
		conf.setReducerClass(AdjustMateEdgeReducer.class);

		//delete the output directory if it exists already
		FileSystem.get(conf).delete(new Path(outputPath), true);

		return JobClient.runJob(conf);
	}


	public int run(String[] args) throws Exception
	{
		String inputPath  = "";
		String outputPath = "";

		run(inputPath, outputPath, 0, 0);

		return 0;
	}

	public static void main(String[] args) throws Exception
	{
		int res = ToolRunner.run(new Configuration(), new AdjustMateEdge(), args);
		System.exit(res);
	}
}

