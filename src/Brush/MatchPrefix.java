/*
    MatchPrefix.java
    2012 Ⓒ CloudBrush, developed by Chien-Chih Chen (rocky@iis.sinica.edu.tw), 
    released under Apache License 2.0 (http://www.apache.org/licenses/LICENSE-2.0) 
    at: https://github.com/ice91/CloudBrush
*/

package Brush;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.URI;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
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
import org.apache.hadoop.filecache.DistributedCache;
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
		private static Path[] localFiles;
		private static HashSet<String> HKmer_List = new HashSet<String>();
		
		public void configure(JobConf job)
		{
			K = Integer.parseInt(job.get("K"));
			try {
                localFiles = DistributedCache.getLocalCacheFiles(job);
            } catch (IOException ioe) {
                System.err.println("Caught exception while getting cached files: " + ioe.toString());
            }
            try {
            	String str;
            	String str_r;
            	File folder = new File(localFiles[0].toString());
            	for (final File fileEntry : folder.listFiles()) {
            		//\\
            		//\\read distributed cache file : load data from file to id_seq   
                    RandomAccessFile f = new RandomAccessFile(fileEntry.getAbsolutePath(), "r");
                    FileChannel ch = f.getChannel();
                    //String first_line = "BBBEBBBBBBBB	";
                    //int readlen = first_line.length(); // to determine the size of one record in the file
                    int readlen = Math.round((float)K/(float)2)+1;
                    byte[] buffer = new byte[readlen];
                    long buff_start = 0; //store buffer start position
                    int buff_pos = 0; // fetch data from this buffer position
                    
                    // determine the buffer size
                    long read_content = Math.min(Integer.MAX_VALUE, ch.size() - buff_start); //0x8FFFFFF = 128MB
                    MappedByteBuffer mb = ch.map(FileChannel.MapMode.READ_ONLY, buff_start, read_content);
                    String kmer;
                    while ( mb.hasRemaining( ) && mb.position()+readlen <= read_content ) {
                    	mb.get(buffer);
                    	kmer = new String(buffer);
                    	str = Node.dna2str(kmer.trim());
	            		str_r = Node.rc(str);
	            		HKmer_List.add(str);
	            		HKmer_List.add(str_r);
                    }
                    ch.close();
                    f.close();
            		//\\
                }
            } catch (IOException ioe){
            	System.err.println("Caught exception while reading cached files: " + ioe.toString());
            }
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
                String prefix_tmp_r = Node.rc(prefix_tmp);
                if (prefix_tmp.compareTo(prefix_tmp_r) < 0) {
                    String prefix = Node.str2dna(prefix_tmp);
                    //output.collect(new Text(prefix), new Text(node.getNodeId() + "\t"  + "f" + "\t" + Node.NODEMSG + "\t" + node.len() + "\t" + Node.str2dna(node.str().substring(K)) +  "\t" + node.cov() ));
                    output.collect(new Text(prefix), new Text(node.getNodeId() + "\t"  + "f0" + "\t" + node.toNodeMsg() ));
                } else if (prefix_tmp_r.compareTo(prefix_tmp) < 0) {
                    String prefix_r = Node.str2dna(prefix_tmp_r);
                    //output.collect(new Text(prefix_r), new Text(node.getNodeId() + "\t"  + "r" + "\t" + Node.NODEMSG + "\t" + node.len() + "\t" + Node.str2dna(node.str().substring(K)) +  "\t" + node.cov() ));
                    output.collect(new Text(prefix_r), new Text(node.getNodeId() + "\t"  + "r1" + "\t" + node.toNodeMsg() ));
                }
                              
                String prefix_rc_tmp = Node.rc(node.str().substring(node.len()-K));
                String prefix_rc_tmp_r = node.str().substring(node.len()-K);
                if (prefix_rc_tmp.compareTo(prefix_rc_tmp_r) < 0) {
                    String prefix_rc = Node.str2dna(prefix_rc_tmp);
                    //output.collect(new Text(prefix_rc), new Text(node.getNodeId() + "\t" + "r" + "\t"+ Node.NODEMSG + "\t" + node.len() + "\t" + Node.str2dna(node.str().substring(0, node.len()-K)) + "\t" + node.cov() ));
                    output.collect(new Text(prefix_rc), new Text(node.getNodeId() + "\t" + "r0" + "\t"+ node.toNodeMsg() ));
                } else if (prefix_rc_tmp_r.compareTo(prefix_rc_tmp) < 0) {
                    String prefix_rc_r = Node.str2dna(prefix_rc_tmp_r);
                    output.collect(new Text(prefix_rc_r), new Text(node.getNodeId() + "\t" + "f1" + "\t"+ node.toNodeMsg() ));
                }
                
                //output.collect(new Text(prefix), new Text(node.getNodeId() + "\t"  + "f" + "\t" + Node.NODEMSG + "\t" + node.len() + "\t" + Node.str2dna(node.str().substring(K)) +  "\t" + node.cov() ));
                //output.collect(new Text(prefix_rc), new Text(node.getNodeId() + "\t" + "r" + "\t"+ Node.NODEMSG + "\t" + node.len() + "\t" + Node.str2dna(node.str().substring(0, node.len()-K)) + "\t" + node.cov() ));
                
                
                reporter.incrCounter("Brush", "nodes", 1);
                //slide the K-mer windows for each read in both strands
                int end = node.len() - K;
                for (int i = 1; i < end; i++)
                {
                    String window_tmp = node.str().substring(i,   i+K);
                    String window_tmp_r = Node.rc(window_tmp);
                    // H-kmer filter
                    if (HKmer_List.contains(window_tmp)) {
                    	reporter.incrCounter("Brush", "hkmer", 1);
                    	continue;
                    }
                    //\\
                    if (window_tmp.compareTo(window_tmp_r) < 0) {
                        String window = Node.str2dna(window_tmp);
                        int overlap_size_f = node.len() - i;
                        if (overlap_size_f >= K && overlap_size_f <= node.len() && !window_tmp.matches("A*") && !window_tmp.matches("T*")) {
                            output.collect(new Text(window),
                                           new Text(node.getNodeId() + "\t" + "f" + "\t" + Node.SUFFIXMSG + "\t" + overlap_size_f + "\t" + node.cov() + "\t" + (node.len()+K)));
                        }
                    } else if (window_tmp_r.compareTo(window_tmp) < 0) {
                        String window_r = Node.str2dna(window_tmp_r);
                        int overlap_size_r = node.len() - (node.len() - K - i);
                        if (overlap_size_r >= K && overlap_size_r <= node.len() && !window_tmp_r.matches("A*") && !window_tmp_r.matches("T*")) {
                            output.collect(new Text(window_r),
                                           new Text(node.getNodeId() + "\t" + "r" + "\t" + Node.SUFFIXMSG + "\t" + overlap_size_r + "\t" + node.cov()+ "\t" + (node.len()+K)));
                        }
                    }
                    /*String window_r_tmp = Node.rc(node.str().substring(node.len() - K - i, node.len() - i));
                    //String window_r_tmp = Node.rc(window_tmp);
                    String window = Node.str2dna(window_tmp);
                    String window_r = Node.str2dna(window_r_tmp);
                    //int remained_base = node.len() - K - i;
                    int overlap_size_f = node.len() - i;
                    int overlap_size_r = node.len() - i;
                    //int overlap_size_r = node.len() - i;
                    if (overlap_size_f >= K && overlap_size_f <= node.len() && !window_tmp.matches("A*") && !window_tmp.matches("T*") && !window_tmp.equals(window_r_tmp)) {
                        output.collect(new Text(window),
                                       new Text(node.getNodeId() + "\t" + "f" + "\t" + Node.SUFFIXMSG + "\t" + overlap_size_f + "\t" + node.cov()));
                    }
                    if (overlap_size_r >= K && overlap_size_r <= node.len() && !window_r_tmp.matches("A*") && !window_r_tmp.matches("T*")&& !window_tmp.equals(window_r_tmp) ) {
                        output.collect(new Text(window_r),
                                       new Text(node.getNodeId() + "\t" + "r" + "\t" + Node.SUFFIXMSG + "\t" + overlap_size_r + "\t" + node.cov()));
                    }*/
                }
            }
		}
	}

	public static class MatchPrefixReducer extends MapReduceBase
	implements Reducer<Text, Text, Text, Text>
	{
		private static int K = 0;
        private static long HighKmer = 0;
        private static long LowKmer = 0;
      
		public void configure(JobConf job) {
			K = Integer.parseInt(job.get("K"));
            HighKmer = Long.parseLong(job.get("UP_KMER"));
            LowKmer = Long.parseLong(job.get("LOW_KMER"));  
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
            //Map<String, Node> nodes = new HashMap<String, Node>();
			Map<String, Map<String, Node>> idx_nodes = new HashMap<String, Map<String, Node>>();
            Map<String, Node> nodes;
            Map<String, Node> emit_nodes = new HashMap<String, Node>();
            //List<EdgeInfo> elist = new ArrayList<EdgeInfo>();
            Map<String, List<EdgeInfo>> idx_elist = new HashMap<String, List<EdgeInfo>>();
            List<EdgeInfo> elist;

            int prefix_sum = 0;
            int belong_read = 0;
            int kmer_count = 0;
            //List<String> ReadID_list = new ArrayList<String>();
            while(iter.hasNext())
			{
				String msg = iter.next().toString();

				String [] vals = msg.split("\t");
                float tmp_cov=0;
				if (vals[2].equals(Node.NODEMSG))
				{
                    
                    Node node = new Node(vals[0]);
                    node.parseNodeMsg(vals, 2);
                    String window = Node.dna2str(prefix.toString());
                    String revers_prefix = Node.str2dna(Node.rc(window));
                    String edge_type = vals[1].substring(0,1);
                    String rev_idx = vals[1].substring(1,2);
                    //EdgeInfo ei = new EdgeInfo(vals[0], vals[1].substring(0, 1), node.len(), node.cov());
                    //nodes.put(node.getNodeId() + "|" + edge_type, node);
                    //\\
                    if (rev_idx.equals("0")){
                        if (idx_nodes.containsKey(prefix.toString())) {
                            nodes = idx_nodes.get(prefix.toString());
                            nodes.put(node.getNodeId() + "|" + edge_type, node);
                            idx_nodes.put(prefix.toString(), nodes); 
                        } else {
                            nodes = new HashMap<String, Node>();
                            nodes.put(node.getNodeId() + "|" + edge_type, node);
                            idx_nodes.put(prefix.toString(), nodes);

                        }
                    } else {
                        if (idx_nodes.containsKey(revers_prefix)) {
                            nodes = idx_nodes.get(revers_prefix);
                            nodes.put(node.getNodeId() + "|" + Node.flip_dir(edge_type), node);
                            idx_nodes.put(revers_prefix, nodes); 
                        } else {
                            nodes = new HashMap<String, Node>();
                            nodes.put(node.getNodeId() + "|" + Node.flip_dir(edge_type), node);
                            idx_nodes.put(revers_prefix, nodes);
                        }
                    }
                    EdgeInfo ei = new EdgeInfo(vals[0], vals[1].substring(0, 1), node.len(), node.cov());
                    if (idx_elist.containsKey(revers_prefix)) {
                        elist = idx_elist.get(prefix.toString());
                        elist.add(ei);
                        idx_elist.put(prefix.toString(),elist);
                    } else {
                        elist = new ArrayList<EdgeInfo>();
                        elist.add(ei);
                        idx_elist.put(prefix.toString(),elist);
                    }
                    //duplicate reverse edge
                    ei = new EdgeInfo(vals[0], Node.flip_dir(vals[1].substring(0, 1)), K, node.cov());
                    if (idx_elist.containsKey(revers_prefix)) {
                        elist = idx_elist.get(revers_prefix);
                        elist.add(ei);
                        idx_elist.put(revers_prefix,elist);
                    } else {
                        elist = new ArrayList<EdgeInfo>();
                        elist.add(ei);
                        idx_elist.put(revers_prefix,elist);
                    }
                    //\\
                    //EdgeInfo ei = new EdgeInfo(vals[0], vals[1], node.len(), node.cov());
                    //elist.add(ei);
				}
				else if (vals[2].equals(Node.SUFFIXMSG))
				{
                    //\\// ReadID + "\t" + "r" + "\t" + Node.SUFFIXMSG  + "\t" + overlap_size
                    tmp_cov = Float.parseFloat(vals[4]);
                    EdgeInfo ei = new EdgeInfo(vals[0], vals[1], Integer.parseInt(vals[3]), Float.parseFloat(vals[4]));
                    if (idx_elist.containsKey(prefix.toString())) {
                        elist = idx_elist.get(prefix.toString());
                        elist.add(ei);
                        idx_elist.put(prefix.toString(),elist);
                    } else {
                        elist = new ArrayList<EdgeInfo>();
                        elist.add(ei);
                        idx_elist.put(prefix.toString(),elist);
                    }
                    //\\// duplicate reverse complement kmer
                    String window = Node.dna2str(prefix.toString());
                    String revers_prefix = Node.str2dna(Node.rc(window));
                    ei = new EdgeInfo(vals[0], Node.flip_dir(vals[1]), Integer.parseInt(vals[5])-Integer.parseInt(vals[3]), Float.parseFloat(vals[4]));
                    if (idx_elist.containsKey(revers_prefix)) {
                        elist = idx_elist.get(revers_prefix);
                        elist.add(ei);
                        idx_elist.put(revers_prefix,elist);
                    } else {
                        elist = new ArrayList<EdgeInfo>();
                        elist.add(ei);
                        idx_elist.put(revers_prefix,elist);
                    }
                    //\\
				}
				else
				{
					throw new IOException("Unknown msgtype: " + msg);
				}
                kmer_count = kmer_count + (int)tmp_cov;
				prefix_sum = prefix_sum + (int)tmp_cov;
			}
            
            
            for(String idx : idx_nodes.keySet())
            {// for [forward and reverse]
            elist=idx_elist.get(idx);
            nodes=idx_nodes.get(idx);
            
            Map<String, List<String>> edges_list = new HashMap<String, List<String>>();
            if (elist.size() > LowKmer /*&& prefix_sum < HighKmer*/ ){
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
            } 

            for(String nodeid_dir : nodes.keySet())
			{
                String dir = nodeid_dir.substring(nodeid_dir.indexOf("|")+1);
				Node node_idx = nodes.get(nodeid_dir);
                Node node = emit_nodes.get(node_idx.getNodeId());
                if (node == null) {
                    node = node_idx;
                }
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
                                if (!node.hasEdge(key, edge_id, overlap_size)) {
                                    node.addEdge(key, edges.get(i).substring(0,edges.get(i).indexOf("|")));
                                }
                               // output.collect(new Text(Math.log(10+node.cov()+cov)*Math.log((double)All_Reads/(double)belong_read)+""), new Text(Math.log(10*node.cov()*cov)*Math.log((double)All_Reads/(double)belong_read)+""));
                            }
                        }
                    }
                }
                //\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\
               
                emit_nodes.put(node.getNodeId(), node);
                //output.collect(new Text(node.getNodeId()), new Text(node.toNodeMsg()));
                reporter.incrCounter("Brush", "nodecount", 1);
			}
		} // for [forward and reverse]
        //\\\
            for(String id : emit_nodes.keySet()){
                Node node = emit_nodes.get(id);
                output.collect(new Text(node.getNodeId()), new Text(node.toNodeMsg()));
            }
        //\\\
        } 
	}



	public RunningJob run(String inputPath, String outputPath, String hkmerlist) throws Exception
	{
		sLogger.info("Tool name: MatchPrefix");
		sLogger.info(" - input: "  + inputPath);
		sLogger.info(" - output: " + outputPath);

		JobConf conf = new JobConf(MatchPrefix.class);
		conf.setJobName("MatchPrefix " + inputPath + " " + BrushConfig.K);
		//\\
        DistributedCache.addCacheFile(new URI(hkmerlist), conf);
        //\\
		

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

		run(inputPath, outputPath, "");

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

