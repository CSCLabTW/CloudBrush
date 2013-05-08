/*
    The file is derived from Contrail Project which is developed by Michael Schatz, 
    Jeremy Chambers, Avijit Gupta, Rushil Gupta, David Kelley, Jeremy Lewi, 
    Deepak Nettem, Dan Sommer, Mihai Pop, Schatz Lab and Cold Spring Harbor Laboratory, 
    and is released under Apache License 2.0 at: 
    http://sourceforge.net/apps/mediawiki/contrail-bio/
*/
package Brush;

import java.io.IOException;
import java.util.HashSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;
import java.util.ArrayList;
import java.util.Map;
import java.util.List;


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


public class Graph2Sfa extends Configured implements Tool
{
	private static final Logger sLogger = Logger.getLogger(Graph2Sfa.class);


	// Graph2SfaMapper
	///////////////////////////////////////////////////////////////////////////

	private static class Graph2SfaMapper extends MapReduceBase
    implements Mapper<LongWritable, Text, Text, Text>
	{
		public void map(LongWritable lineid, Text nodetxt,
                OutputCollector<Text, Text> output, Reporter reporter)
                throws IOException
        {
			Node node = new Node();
			node.fromNodeMsg(nodetxt.toString());

			

			String str = node.str();

			//output.collect(new Text(">" + node.getNodeId()), new Text("len=" + str.length() + "\tcov=" + node.cov()));
            if (/*!node.hasCustom("n")*/ !node.isUnique()) {
                if (node.cov() == 1) {
                    output.collect(new Text( node.getNodeId()), new Text(str));
                    reporter.incrCounter("Brush", "nodes", 1);
                } else {
                    for(int i=1; i < node.cov(); i++) {
                        output.collect(new Text( i + "_" + node.getNodeId()), new Text(str));
                        reporter.incrCounter("Brush", "nodes", 1);
                    }
                }
            }
			/*int LINE_LEN = 60;

			for (int i = 0; i < str.length(); i+=LINE_LEN)
			{
				int end = i + LINE_LEN;
				if (end > str.length()) { end = str.length();}

				String line = str.substring(i, end);
				output.collect(new Text(line), null);
			}*/
        }
	}


	// Run Tool
	///////////////////////////////////////////////////////////////////////////

	public RunningJob run(String inputPath, String outputPath) throws Exception
	{
		sLogger.info("Tool name: Graph2Sfa");
		sLogger.info(" - input: "  + inputPath);
		sLogger.info(" - output: " + outputPath);

		JobConf conf = new JobConf(Graph2Sfa.class);
		conf.setJobName("Graph2Sfa " + inputPath);

		FileInputFormat.addInputPath(conf, new Path(inputPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(Graph2SfaMapper.class);

		BrushConfig.initializeConfiguration(conf);
		conf.setNumReduceTasks(0);

		//delete the output directory if it exists already
		FileSystem.get(conf).delete(new Path(outputPath), true);

		return JobClient.runJob(conf);
	}


	// Parse Arguments and run
	///////////////////////////////////////////////////////////////////////////

	public int run(String[] args) throws Exception
	{
		//String inputPath  = "/users/mschatz/contrail/Ec100k/99-final/";
		//String outputPath = "/users/mschatz/contrail/Ec100k/99-final.fa/";

		String inputPath  = "/users/mschatz/contrail/Ec100k/04-notipscmp";
		String outputPath = "/users/mschatz/contrail/Ec100k/04-notipscmp.fa";

		run(inputPath, outputPath);
		return 0;
	}


	// Main
	///////////////////////////////////////////////////////////////////////////

	public static void main(String[] args) throws Exception
	{
		int res = ToolRunner.run(new Configuration(), new Graph2Sfa(), args);
		System.exit(res);
	}
}
