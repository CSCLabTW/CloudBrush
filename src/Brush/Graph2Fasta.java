/*
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


public class Graph2Fasta extends Configured implements Tool
{
	private static final Logger sLogger = Logger.getLogger(Graph2Fasta.class);


	// Graph2FastaMapper
	///////////////////////////////////////////////////////////////////////////

	private static class Graph2FastaMapper extends MapReduceBase
    implements Mapper<LongWritable, Text, Text, Text>
	{
		public void map(LongWritable lineid, Text nodetxt,
                OutputCollector<Text, Text> output, Reporter reporter)
                throws IOException
        {
			Node node = new Node();
			node.fromNodeMsg(nodetxt.toString());

			reporter.incrCounter("Brush", "nodes", 1);

			String str = node.str();

			output.collect(new Text(">" + node.getNodeId()), new Text("len=" + str.length() + "\tcov=" + node.cov()));

			int LINE_LEN = 60;

			for (int i = 0; i < str.length(); i+=LINE_LEN)
			{
				int end = i + LINE_LEN;
				if (end > str.length()) { end = str.length();}

				String line = str.substring(i, end);
				output.collect(new Text(line), null);
			}
        }
	}


	// Run Tool
	///////////////////////////////////////////////////////////////////////////

	public RunningJob run(String inputPath, String outputPath) throws Exception
	{
		sLogger.info("Tool name: Graph2Fasta");
		sLogger.info(" - input: "  + inputPath);
		sLogger.info(" - output: " + outputPath);

		JobConf conf = new JobConf(Graph2Fasta.class);
		conf.setJobName("Graph2Fasta " + inputPath);

		FileInputFormat.addInputPath(conf, new Path(inputPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(Graph2FastaMapper.class);

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
		String inputPath  = "";
		String outputPath = "";

		run(inputPath, outputPath);
		return 0;
	}


	// Main
	///////////////////////////////////////////////////////////////////////////

	public static void main(String[] args) throws Exception
	{
		int res = ToolRunner.run(new Configuration(), new Graph2Fasta(), args);
		System.exit(res);
	}
}
