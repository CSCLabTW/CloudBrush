/*
    Compressible.java
    2012 Ⓒ CloudBrush, developed by Chien-Chih Chen (rocky@iis.sinica.edu.tw), 
    released under Apache License 2.0 (http://www.apache.org/licenses/LICENSE-2.0) 
    at: https://github.com/ice91/CloudBrush

    The file is derived from Contrail Project which is developed by Michael Schatz, 
    Jeremy Chambers, Avijit Gupta, Rushil Gupta, David Kelley, Jeremy Lewi, 
    Deepak Nettem, Dan Sommer, Mihai Pop, Schatz Lab and Cold Spring Harbor Laboratory, 
    and is released under Apache License 2.0 at: 
    http://sourceforge.net/apps/mediawiki/contrail-bio/
*/
package Brush;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
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


public class Compressible extends Configured implements Tool
{
	private static final Logger sLogger = Logger.getLogger(Compressible.class);

	public static class CompressibleMapper extends MapReduceBase
    implements Mapper<LongWritable, Text, Text, Text>
	{
		public void map(LongWritable lineid, Text nodetxt,
                OutputCollector<Text, Text> output, Reporter reporter)
                throws IOException
        {
			Node node = new Node();
			node.fromNodeMsg(nodetxt.toString());

			for (String adj : Node.dirs)
			{
				node.setCanCompress(adj, false);

				TailInfo next = node.gettail(adj);

				if (next != null /*&& node.getBlackEdges() == null*/)
				{
					if (next.id.equals(node.getNodeId())) { continue; }

					reporter.incrCounter("Brush", "remotemark", 1);

					output.collect(new Text(next.id),
					               new Text(Node.HASUNIQUEP + "\t" + node.getNodeId() + "\t" + adj));
				}
			}

			output.collect(new Text(node.getNodeId()), new Text(node.toNodeMsg()));

			reporter.incrCounter("Brush", "nodes", 1);
        }
	}

	public static class CompressibleReducer extends MapReduceBase
	implements Reducer<Text, Text, Text, Text>
	{
		public void reduce(Text key, Iterator<Text> iter,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException
		{
			Node node = new Node(key.toString());
		    Set<String> f_unique = new HashSet<String>();
			Set<String> r_unique = new HashSet<String>();

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
				else if (vals[0].equals(Node.HASUNIQUEP))
				{
					if      (vals[2].equals("f")) { f_unique.add(vals[1]); }
					else if (vals[2].equals("r")) { r_unique.add(vals[1]); }
				}
				else
				{
					throw new IOException("Unknown msgtype: " + msg);
				}
			}

			if (sawnode != 1)
            {
                throw new IOException("ERROR: Didn't see exactly 1 nodemsg (" + sawnode + ") for " + key.toString());
			}

            for (String adj : Node.dirs)
			{
				TailInfo next = node.gettail(adj);

				if (next != null)
				{
					if ((next.dir.equals("f") && r_unique.contains(next.id)) ||
						(next.dir.equals("r") && f_unique.contains(next.id)))
					{
                        // for path compress
                        if (node.getBlackEdges() == null) {
                            node.setCanCompress(adj, true);
                        }
						reporter.incrCounter("Brush", "compressible", 1);
					}
				}
			}
           // System.err.println(node.getNodeId() + " " + node.toNodeMsg() );
			output.collect(new Text(node.getNodeId()), new Text(node.toNodeMsg()));
		}
	}


	public RunningJob run(String inputPath, String outputPath) throws Exception
	{
		sLogger.info("Tool name: Compressible");
		sLogger.info(" - input: "  + inputPath);
		sLogger.info(" - output: " + outputPath);

		//JobConf conf = new JobConf(Stats.class);
        JobConf conf = new JobConf(Compressible.class);
		conf.setJobName("Compressible " + inputPath);

		BrushConfig.initializeConfiguration(conf);

		FileInputFormat.addInputPath(conf, new Path(inputPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(CompressibleMapper.class);
		conf.setReducerClass(CompressibleReducer.class);

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
		int res = ToolRunner.run(new Configuration(), new Compressible(), args);
		System.exit(res);
	}
}
