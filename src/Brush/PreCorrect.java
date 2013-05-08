/*
    PreCorrect.java
    2012 Ⓒ CloudBrush, developed by Chien-Chih Chen (rocky@iis.sinica.edu.tw), 
    released under Apache License 2.0 (http://www.apache.org/licenses/LICENSE-2.0) 
    at: https://github.com/ice91/CloudBrush
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


public class PreCorrect extends Configured implements Tool
{
	private static final Logger sLogger = Logger.getLogger(PreCorrect.class);

	public static class PreCorrectMapper extends MapReduceBase
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
           
            //slide the split K-mer windows for each read in both strands
            int end = node.len() - 25;
            for (int i = 0; i < end; i++)
            {
                String window_tmp = node.str().substring(i, i+12) + node.str().substring(i+13, i+25);
                String window_r_tmp = Node.rc(node.str().substring(node.len()-25-i, node.len()-13-i) + node.str().substring(node.len()-12-i, node.len()-i));
                //String window_r_tmp = Node.rc(window_tmp);
                String window = Node.str2dna(window_tmp);
                String window_r = Node.str2dna(window_r_tmp);
                //int remained_base = node.len() - K - i;
                int f_pos = i + 12;
                int r_pos = node.len()-13-i;
                //int overlap_size_r = node.len() - i;
                if ( !window_tmp.matches("A*") && !window_tmp.matches("T*") ) {
                    output.collect(new Text(window),
                                   new Text(node.getNodeId() + "\t" + "f" + "\t" + f_pos + "\t" + node.str().charAt(f_pos) + "\t" + node.cov()));
                }
                if (!window_tmp.matches("A*") && !window_tmp.matches("T*") ) {
                    output.collect(new Text(window_r),
                                   new Text(node.getNodeId() + "\t" + "r" + "\t" + r_pos + "\t" + Node.rc(node.str().charAt(r_pos) + "") + "\t" +node.cov()));
                }
            }
            
		}
	}

	public static class PreCorrectReducer extends MapReduceBase
	implements Reducer<Text, Text, Text, Text>
	{
		private static int K = 0;

		public void configure(JobConf job) {
			K = Integer.parseInt(job.get("K"));
		}

        public class ReadInfo
		{
			public String id;
            public String dir;
			public int pos;
            public String base;
            public float cov;

			public ReadInfo(String id1, String dir1, int pos1, String base1, float cov1) throws IOException
			{
				id = id1;
                dir = dir1;
                pos = pos1;
                base = base1;
                cov = cov1;
			}

            public String toString()
			{
				return id + "!" + dir + "|" + pos + "|" + base;
			}
		}
        
		public void reduce(Text prefix, Iterator<Text> iter,
						   OutputCollector<Text, Text> output, Reporter reporter)
						   throws IOException
		{
            Map<String, Node> nodes = new HashMap<String, Node>();
			List<ReadInfo> readlist = new ArrayList<ReadInfo>();
            Map<String, List<String>> edges_list = new HashMap<String, List<String>>();

            int prefix_sum = 0;
            int belong_read = 0;
            int kmer_count = 0;
            List<String> ReadID_list = new ArrayList<String>();
            
            //\\ 0:A 1:T 2:C 3:G 4:Sum
            int[] base_array = new int[5];
            for(int i=0; i<5; i++) {
                base_array[i] = 0;
            }
            while(iter.hasNext())
			{
				String msg = iter.next().toString();
				String [] vals = msg.split("\t");
                ReadInfo read_item = new ReadInfo(vals[0],vals[1],Integer.parseInt(vals[2]), vals[3], Float.parseFloat(vals[4]));
                if (read_item.base.equals("A")){
                    base_array[0] = base_array[0] + (int)read_item.cov;
                    base_array[4] = base_array[4] + (int)read_item.cov;
                } else if (read_item.base.equals("T")) {
                    base_array[1] = base_array[1] + (int)read_item.cov;
                    base_array[4] = base_array[4] + (int)read_item.cov;
                } else if (read_item.base.equals("C")) {
                    base_array[2] = base_array[2] + (int)read_item.cov;
                    base_array[4] = base_array[4] + (int)read_item.cov;
                } else if (read_item.base.equals("G")) {
                    base_array[3] = base_array[3] + (int)read_item.cov;
                    base_array[4] = base_array[4] + (int)read_item.cov;
                }
                readlist.add(read_item);
                //output.collect(prefix, new Text(read_item.toString()));
			}
            String correct_base = "N";
            /*float majority = 0.8f;
            if ((float)base_array[0]/(float)base_array[4] > majority) {
                correct_base = "A";
            } else if ((float)base_array[1]/(float)base_array[4] > majority) {
                correct_base = "T";
            } else if ((float)base_array[2]/(float)base_array[4] > majority) {
                correct_base = "C";
            } else if ((float)base_array[3]/(float)base_array[4] > majority) {
                correct_base = "G";
            }*/
            float winner_sum = 0;
            if (base_array[0] > base_array[1] && base_array[0] > base_array[2] && base_array[0] > base_array[3]) {
                correct_base = "A";
                winner_sum = base_array[0];
            } else if (base_array[1] > base_array[0] && base_array[1] > base_array[2] && base_array[1] > base_array[3] ) {
                correct_base = "T";
                winner_sum = base_array[1];
            } else if (base_array[2] > base_array[0] && base_array[2] > base_array[1] && base_array[2] > base_array[3] ) {
                correct_base = "C";
                winner_sum = base_array[2];
            } else if (base_array[3] > base_array[0] && base_array[3] > base_array[1] && base_array[3] > base_array[2] ) {
                correct_base = "G";
                winner_sum = base_array[3];
            }
            if (!correct_base.equals("N")) {
                for(int i=0; i < readlist.size(); i++) {
                    if (!readlist.get(i).base.equals(correct_base)) {
                        //\\
                        if (readlist.get(i).base.equals("A") && (float)base_array[0]/(float)winner_sum > 0.25f){
                            continue;
                        }
                        if (readlist.get(i).base.equals("T") && (float)base_array[1]/(float)winner_sum > 0.25f){
                            continue;
                        }
                        if (readlist.get(i).base.equals("C") && (float)base_array[2]/(float)winner_sum > 0.25f){
                            continue;
                        }
                        if (readlist.get(i).base.equals("G") && (float)base_array[3]/(float)winner_sum > 0.25f){
                            continue;
                        }
                        //\\
                        if (readlist.get(i).dir.equals("f")) {
                            String correct_msg =  readlist.get(i).pos + "," + correct_base;
                            output.collect(new Text(readlist.get(i).id), new Text(correct_msg));
                            reporter.incrCounter("Brush", "fix_char", 1);
                        } if (readlist.get(i).dir.equals("r")) {
                            String correct_msg = /*readlist.get(i).id + "|" +*/ readlist.get(i).pos + "," + Node.rc(correct_base);
                            output.collect(new Text(readlist.get(i).id), new Text(correct_msg));
                            reporter.incrCounter("Brush", "fix_char", 1);
                        }
                    }
                }
            }
		}
	}



	public RunningJob run(String inputPath, String outputPath) throws Exception
	{
		sLogger.info("Tool name: PreCorrect");
		sLogger.info(" - input: "  + inputPath);
		sLogger.info(" - output: " + outputPath);

		JobConf conf = new JobConf(PreCorrect.class);
		conf.setJobName("PreCorrect " + inputPath + " " + BrushConfig.K);

		BrushConfig.initializeConfiguration(conf);

		FileInputFormat.addInputPath(conf, new Path(inputPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(PreCorrectMapper.class);
		conf.setReducerClass(PreCorrectReducer.class);

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
		int res = ToolRunner.run(new Configuration(), new PreCorrect(), args);
		System.exit(res);
	}
}

