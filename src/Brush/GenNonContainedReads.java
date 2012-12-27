/*
    GenNonContainedReads.java
    2012 Ⓒ CloudBrush, developed by Chien-Chih Chen (rocky@iis.sinica.edu.tw), 
    released under Apache License 2.0 (http://www.apache.org/licenses/LICENSE-2.0) 
    at: https://github.com/ice91/CloudBrush
*/

package Brush;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
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


public class GenNonContainedReads extends Configured implements Tool
{
	private static final Logger sLogger = Logger.getLogger(MatchPrefix.class);

	public static class GenNonContainedReadsMapper extends MapReduceBase
    implements Mapper<LongWritable, Text, Text, Text>
	{
		public static int K = 21;
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
			String[] fields = nodetxt.toString().split("\t");

			if (fields.length != 2 && fields.length != 3 )
			{
				//System.err.println("Warning: invalid input: \"" + nodetxt.toString() + "\"");
				reporter.incrCounter("Brush", "input_lines_invalid", 1);
				return;
			}

			String tag = fields[0];

			tag.replaceAll(" ", "_");
			tag.replaceAll(":", "_");
			tag.replaceAll("#", "_");
			tag.replaceAll("-", "_");
			tag.replaceAll(".", "_");

			String seq = fields[1].toUpperCase();

			// Hard chop a few bases off of each end of the read
			if (TRIM5 > 0 || TRIM3 > 0)
			{
				// System.err.println("orig: " + seq);
				seq = seq.substring(TRIM5, seq.length() - TRIM5 - TRIM3);
				// System.err.println("trim: " + seq);
			}

			// Automatically trim Ns off the very ends of reads
			/*int endn = 0;
			while (endn < seq.length() && seq.charAt(seq.length()-1-endn) == 'N') { endn++; }
			if (endn > 0) { seq = seq.substring(0, seq.length()-endn); }

			int startn = 0;
			while (startn < seq.length() && seq.charAt(startn) == 'N') { startn++; }
			if (startn > 0 && (seq.length() - startn) > startn) {
                seq = seq.substring(startn, seq.length() - startn);
            }*/
            //if (startn > 0) { seq = seq.substring(startn, seq.length() - startn); }

			// Check for non-dna characters
			if (seq.matches(".*[^ACGT].*"))
			{
				//System.err.println("WARNING: non-DNA characters found in " + tag + ": " + seq);
				reporter.incrCounter("Brush", "reads_skipped", 1);
				return;
			}

			// check for short reads
			if (seq.length() <= K)
			{
				//System.err.println("WARNING: read " + tag + " is too short: " + seq);
				reporter.incrCounter("Brush", "reads_short", 1);
				return;
			}

			// Now emit the prefix of the reads
            Node node = new Node(tag);
            String prefix_tmp = seq.substring(0, K);
            String prefix = Node.str2dna(prefix_tmp);
            node.setstr(seq);
            node.setCoverage(1);
            output.collect(new Text(prefix), new Text("r" + "\t" + node.toNodeMsg(true)));
            String prefix_rc_tmp = Node.rc(seq).substring(0, K);
            String prefix_rc = Node.str2dna(prefix_rc_tmp);
            output.collect(new Text(prefix_rc), new Text("f" + "\t" + node.toNodeMsg(true)));

			reporter.incrCounter("Brush", "reads_good", 1);
			reporter.incrCounter("Brush", "reads_goodbp", seq.length());
		}
	}

	public static class GenNonContainedReadsReducer extends MapReduceBase
	implements Reducer<Text, Text, Text, Text>
	{
		private static int K = 0;

		public void configure(JobConf job) {
			K = Integer.parseInt(job.get("K"));
		}

		public void reduce(Text prefix, Iterator<Text> iter,
						   OutputCollector<Text, Text> output, Reporter reporter)
						   throws IOException
		{
            //Map<String, Node> nodes = new HashMap<String, Node>();
            List<Node> nodes = new ArrayList<Node>();
            Map<String, Node> nodes_fr = new HashMap<String, Node>();
			while(iter.hasNext())
			{
				String msg = iter.next().toString();

				String [] vals = msg.split("\t");

				if (vals[2].equals(Node.NODEMSG))
				{
                    Node node = new Node(vals[1]);
                    //node.fromNodeMsg(msg);
                    node.parseNodeMsg(vals, 2);
                    nodes_fr.put(node.getNodeId() + "|" + vals[0], node);
                    if (vals[0].equals("f")) {
                        //nodes.put(node.getNodeId(), node);
                        nodes.add(node);
                    }
				}
				else
				{
					throw new IOException("Unknown msgtype: " + msg);
				}
			}

            //Map<String, Node> Contained_Nodes = new HashMap<String, Node>();
            //Map<String, Node> Filter_Nodes = new HashMap<String, Node>();
            for (int i = 0; i < nodes.size(); i++) {
                Node node = nodes.get(i);
                for(String nodeid_dir : nodes_fr.keySet())
                {
                    //System.err.println(nodeid_dir + " XX");
                    String id = nodeid_dir.substring(0, nodeid_dir.indexOf("|"));
                    String dir = nodeid_dir.substring(nodeid_dir.indexOf("|")+1);
                    if (node.getNodeId().equals(id)) {
                        continue;
                    }
                    Node verify_node = nodes_fr.get(nodeid_dir);
                    String verify_str;
                    if (dir.equals("f")){
                        verify_str = verify_node.str();
                    } else {
                        verify_str = Node.rc(verify_node.str());
                    }


                    if (node.str().equals(verify_str)) {
                        if (node.getNodeId().compareTo(verify_node.getNodeId()) < 0) {
                            node.setCoverage(node.cov()+1);
                            node.addPairEnd(verify_node.getNodeId());
                        } else {
                            node.setCustom("n", "1"); // n = CONTAINED
                            reporter.incrCounter("Brush", "contained", 1);
                            break;
                        }
                    } else {
                       /* int distance = Node.getLevenshteinDistance(node.str(),verify_str);
                        if (node.str().length() > verify_str.length()) {
                            if ((float)distance/(float)node.str().length() < 0.03f) {
                                if (node.getNodeId().compareTo(verify_node.getNodeId()) < 0) {
                                    node.setCoverage(node.cov()+1);
                                    node.addPairEnd(verify_node.getNodeId());
                                } else {
                                    node.setCustom("n", "1"); // n = CONTAINED
                                    reporter.incrCounter("Brush", "contained", 1);
                                    break;
                                }
                            }
                        } else if (node.str().length() < verify_node.str().length()) {
                            if ((float)distance/(float)verify_node.str().length() < 0.03f) {
                                if (node.getNodeId().compareTo(verify_node.getNodeId()) < 0) {
                                    node.setCoverage(node.cov()+1);
                                } else {
                                    node.setCustom("n", "1"); // n = CONTAINED
                                    reporter.incrCounter("Brush", "contained", 1);
                                    break;
                                }
                            }

                        } else {
                            if ((float)distance/(float)node.str().length() < 0.03f) {
                                if (node.getNodeId().compareTo(verify_node.getNodeId()) < 0) {
                                    node.setCoverage(node.cov()+1);
                                } else {
                                    node.setCustom("n", "1"); // n = CONTAINED
                                    reporter.incrCounter("Brush", "contained", 1);
                                    break;
                                }
                            }
                        }*/
                    }
                    // for different length
                    /*if (node.str().length() > verify_str.length() && node.str().substring(0, verify_str.length()).equals(verify_str)){
                        node.setCoverage(node.cov()+1);
                    } else if (node.str().length() < verify_node.str().length() && verify_str.substring(0, node.str().length()).equals(node.str().length())) {
                        node.setCustom("n", "1"); // n = CONTAINED
                        break;
                    }*/
                }
                output.collect(new Text(node.getNodeId()), new Text(node.toNodeMsg()));
                reporter.incrCounter("Brush", "nodecount", 1);
            }


		}
	}



	public RunningJob run(String inputPath, String outputPath) throws Exception
	{
		sLogger.info("Tool name: GenNonContainedReads");
		sLogger.info(" - input: "  + inputPath);
		sLogger.info(" - output: " + outputPath);

		JobConf conf = new JobConf(Stats.class);
		conf.setJobName("GenNonContainedReads " + inputPath + " " + BrushConfig.K);

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

		conf.setMapperClass(GenNonContainedReadsMapper.class);
		conf.setReducerClass(GenNonContainedReadsReducer.class);

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

		run(inputPath, outputPath);

		long endtime = System.currentTimeMillis();

		float diff = (float) (((float) (endtime - starttime)) / 1000.0);

		System.out.println("Runtime: " + diff + " s");

		return 0;
	}

	public static void main(String[] args) throws Exception
	{
		int res = ToolRunner.run(new Configuration(), new GenNonContainedReads(), args);
		System.exit(res);
	}
}


