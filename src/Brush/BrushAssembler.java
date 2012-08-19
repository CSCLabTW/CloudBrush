/*
    BrushAssembler.java
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

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Enumeration;

import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Appender;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Layout;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.log4j.TTCCLayout;
import org.apache.log4j.helpers.DateLayout;

public class BrushAssembler extends Configured implements Tool
{
    private static DecimalFormat df = new DecimalFormat("0.00");
	private static FileOutputStream logfile;
	private static PrintStream logstream;
	
	JobConf baseconf = new JobConf(BrushAssembler.class);

    //static String preprocess = "00-preprocess";
    //static String preprocesscmp = "01-preprocesscmp";
    //static String prematch = "00-prefix";
    static String preprocess = "00-preprocess";
    static String overlap = "01-overlap";
    static String string = "02-string";
    static String notips = "03-notips";
    static String notipscmp = "04-notipscmp";
    static String nobubbles    = "05-nobubbles";
	static String nobubblescmp = "06-nobubblescmp";
    static String lowcov       = "07-lowcov";
	static String lowcovcmp    = "08-lowcovcmp";
    //static String pairadjust      = "09-pairadjust";
	//static String pairadjustcmp   = "09-pairadjustcmp";
    static String edgeadjust = "09-edgeadjust";
    static String edgeadjustcmp = "10-edgeadjustcmp";
    static String finalcmp     = "99-final";
    

    // Message Management
	///////////////////////////////////////////////////////////////////////////

	long GLOBALNUMSTEPS = 0;
	long JOBSTARTTIME = 0;
	public void start(String desc)
	{
		msg(desc + ":\t");
		JOBSTARTTIME = System.currentTimeMillis();
		GLOBALNUMSTEPS++;
	}

	public void end(RunningJob job) throws IOException
	{
		long endtime = System.currentTimeMillis();
		long diff = (endtime - JOBSTARTTIME) / 1000;

		msg(job.getJobID() + " " + diff + " s");

		if (!job.isSuccessful())
		{
			System.out.println("Job was not successful");
			System.exit(1);
		}
	}

	public static void msg(String msg)
	{
		logstream.print(msg);
		System.out.print(msg);
	}

	public long counter(RunningJob job, String tag) throws IOException
	{
		return job.getCounters().findCounter("Brush", tag).getValue();
	}

    // Stage Management
	///////////////////////////////////////////////////////////////////////////

	boolean RUNSTAGE = false;
	private String CURRENTSTAGE;

	public boolean runStage(String stage)
	{
		CURRENTSTAGE = stage;

		if (BrushConfig.STARTSTAGE == null || BrushConfig.STARTSTAGE.equals(stage))
		{
			RUNSTAGE = true;
		}

		return RUNSTAGE;
	}

	public void checkDone()
	{
		if (BrushConfig.STOPSTAGE != null && BrushConfig.STOPSTAGE.equals(CURRENTSTAGE))
		{
			RUNSTAGE = false;
			msg("Stopping after " + BrushConfig.STOPSTAGE + "\n");
			System.exit(0);
		}
	}

    // File Management
	///////////////////////////////////////////////////////////////////////////

	public void cleanup(String path) throws IOException
	{
		FileSystem.get(baseconf).delete(new Path(path), true);
	}

	public void save_result(String base, String opath, String npath) throws IOException
	{
		//System.err.println("Renaming " + base + opath + " to " + base + npath);

		msg("Save result to " + npath + "\n\n");

		FileSystem.get(baseconf).delete(new Path(base+npath), true);
		FileSystem.get(baseconf).rename(new Path(base+opath), new Path(base+npath));
	}

    // convertFasta
	///////////////////////////////////////////////////////////////////////////

	public void convertFasta(String basePath, String graphdir, String fastadir) throws Exception
	{
        Graph2Fasta g2f = new Graph2Fasta();
        CountReads cr = new CountReads();
		start("convertFasta " + graphdir);
		RunningJob job = g2f.run(basePath + graphdir, basePath + fastadir);
		end(job);
        msg("\n");
	}

    // Compute Graph Statistics
	///////////////////////////////////////////////////////////////////////////

	public void computeStats(String base, String dir) throws Exception
	{
		start("Compute Stats " + dir);
		Stats stats = new Stats();
		RunningJob job = stats.run(base+dir, base+dir+".stats");
		end(job);

		msg("\n\nStats " + dir + "\n");
		msg("==================================================================================\n");

		FSDataInputStream statstream = FileSystem.get(baseconf).open(new Path(base+dir+".stats/part-00000"));
		BufferedReader b = new BufferedReader(new InputStreamReader(statstream));

		String s;
		while ((s = b.readLine()) != null)
		{
			msg(s);
			msg("\n");
		}
		msg("\n");
	}
    
    // preprocess
	///////////////////////////////////////////////////////////////////////////
    public void preprocess(String inputPath, String basePath, String preprocess) throws Exception
	{
		RunningJob job;
        //long trans_edge = 0;
        msg("\nPreProcess:");
        start("\n  Generate nonContained Reads");
        GenNonContainedReads gnc = new GenNonContainedReads();
        job = gnc.run(inputPath , basePath + preprocess + ".0");
        end(job);

        long nodecnt      = counter(job, "nodecount");
        long reads_goodbp = counter(job, "reads_goodbp");
        long reads_good   = counter(job, "reads_good");
        long reads_short  = counter(job, "reads_short");
        long reads_skip   = counter(job, "reads_skipped");
        long reads_all = reads_good + reads_short + reads_skip;

        if (reads_good == 0)
        {
            throw new IOException("No good reads");
        }

        String frac_reads = df.format(100*reads_good/reads_all);
        msg("  " + nodecnt + " nodes [" + reads_good +" (" + frac_reads + "%) good reads, " + reads_goodbp + " bp]");
        //\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\
        long contained = counter(job, "contained");
        msg("  " + contained + " contained_reads");
        start("\n  Remove Redundant Reads");
        RedundantRemoval Rr = new RedundantRemoval();
        job = Rr.run(basePath+preprocess + ".0", basePath+preprocess);
        end(job);
        long redundant = counter(job, "redundant");
        nodecnt      = counter(job, "nodes");
        msg("  " + redundant + " redundants " + nodecnt + " nodes");      
        start("\n  Count Kmer");
        CountKmer ck = new CountKmer();
        job = ck.run(basePath + preprocess+ ".0", basePath + preprocess + ".kmer");
        end(job);
        long allkmer = counter(job, "Allkmer");
        long diffkmer = counter(job, "Diffkmer");
        long nodes = counter(job, "nodes");
        start("\n  Kmer Status");
        KmerStatus ks = new KmerStatus();
        job = ks.run(basePath + preprocess + ".kmer", basePath + preprocess + ".kmer.stats");
        end(job);
        msg("\n");
	}
    
     // build Overlap graph
	///////////////////////////////////////////////////////////////////////////
    public void buildOverlap(String inputPath, String basePath, String preprocess, String overlap) throws Exception
	{
        RunningJob job;
        //long trans_edge = 0;
        msg("\nBuild Overlap Graph:");
        start("\n  Match Prefix");
        MatchPrefix mp = new MatchPrefix();
        job = mp.run(basePath + preprocess, basePath + preprocess + ".prefix", 1, 1, 1);
        end(job);
        start("\n  Verify Overlap");
        VerifyOverlap vo = new VerifyOverlap();
        job = vo.run(basePath + preprocess + ".prefix", basePath + preprocess + ".vo");
        end(job);
        start("\n  Generate Reverse Edges");
        GenReverseEdge ge = new GenReverseEdge();
        job = ge.run(basePath + preprocess + ".vo", basePath + overlap);
        end(job); 
        msg("\n");
    }
    
     // build String graph
	///////////////////////////////////////////////////////////////////////////
    public void buildStringGraph(String inputPath, String basePath, String overlap, String Sgraph) throws Exception
	{
        RunningJob job;
        long trans_edge = 0;
        //\\\\\\\\\\\\
        //\\ Cut Chimeric Links
        long cut_edge = 1;
        long round = 0;
        String current = overlap;
        msg("\nBuild String Graph:");
        while (cut_edge > 0 && round < 2)
        {
            round++;
            start("\n  Cut Chimeric Links " + round);
            CutChimericLinks chl = new CutChimericLinks();
            job = chl.run(basePath + current, basePath + overlap + ".chl");
            long edge_removal = counter(job, "edge_removal");
            msg("  " + edge_removal + " cut_links ");
            end(job);
            //\\
            if (round > 1 && edge_removal == 0) {
                break;
            }
            //\\
            start("\n  Remove Edges");
            EdgeRemoval re = new EdgeRemoval();
            job = re.run(basePath + overlap + ".chl", basePath + overlap + ".chl2");
            end(job);
            cut_edge = edge_removal;
            current = overlap + ".chl2";
        }
        //\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\
        start("\n  Transitive Reduction");
        TransitiveReduction tr = new TransitiveReduction();
        job = tr.run(basePath + overlap + ".chl2", basePath + overlap + ".tr");
        end(job);
        trans_edge = counter(job, "trans_edge");
        long contained_edge = counter(job, "contained_edge");
        msg("  " + trans_edge + " transitive_edge " + contained_edge + " contained_edge\n");
        start("  Remove Edges");
        EdgeRemoval re = new EdgeRemoval();
        job = re.run(basePath + overlap + ".tr", basePath + overlap + ".re");
        end(job);
        
        compressChains(basePath, overlap + ".re", Sgraph);
        //---  
        start("\n  Define Consensus");
        DefineConsensus dc = new DefineConsensus();
        job = dc.run(basePath + Sgraph, basePath + Sgraph + ".dc");
        end(job);
        start("\n  Count Braids");
        CountBraid cb = new CountBraid();
        job = cb.run(basePath + Sgraph + ".dc", basePath + Sgraph + ".cb");
        end(job);
        long nodes = counter(job, "nodes");
        long edges = counter(job, "edges");
        long braids = counter(job, "braids");
        msg("  " + braids + " braids  " + nodes + " nodes  " + edges + " edges\n");
        
    }
    
    // String Graph edge adjustment
	///////////////////////////////////////////////////////////////////////////
    public void edgeAdjustment(String basePath, String input, String startname, String finalname) throws Exception
	{
		RunningJob job;
        long trans_edge = 0;
        long remaining = 1;
        long round = 0;
        String current = input;
        CountReads cr = new CountReads();
        CutRepeatBoundary crb = new CutRepeatBoundary();
        EdgeRemoval re = new EdgeRemoval();
        while (remaining > 0)
		{
			round++;
			String output = startname + "." + round;
			long removed = 0;

			if (BrushConfig.RESTART_TIP_REMAIN > 0)
			{
				remaining = BrushConfig.RESTART_TIP_REMAIN;
				BrushConfig.RESTART_TIP_REMAIN = 0;
				msg("Restart remove tips " + round + ":");
				removed = 123456789;
			}
			else
			{
				//RunningJob job;
                /*start("\nCount Reads ");
                job = cr.run(basePath + current, basePath + startname+".count");
                end(job);*/
                long reads = 1;//counter(job, "reads");
                long ctg_sum = 1;//counter(job,"ctg_sum");
                start("\nEdge Adjustment " + round);
                crb = new CutRepeatBoundary();
                job = crb.run(basePath + current, basePath + output + ".crb", reads, ctg_sum);
                long edge_removal = counter(job, "edge_removal");
                msg("  " + edge_removal + " cut_links ");
                remaining = counter(job, "edge_removal");
                end(job);
                long repeat =  counter(job, "repeat");
                long unique = counter(job, "unique");
                msg("  " + unique + " unique " + repeat + " repeat " );
                start("\n  Remove Edges");
                re = new EdgeRemoval();
                job = re.run(basePath + output + ".crb", basePath + output);
                end(job);
			}
            msg("\n");

			if (remaining > 0)
			{
				if (round > 1) { cleanup(current); }

				current = output + ".cmp";
				compressChains(basePath, output, current);
				remaining = 1;
                cleanup(output);
			}  else {
                current = output;
            }

			//cleanup(output);
		}

		save_result(basePath, current, finalname);
	}

    // Maximally compress chains
	///////////////////////////////////////////////////////////////////////////
	public long compressChains(String basePath, String startname, String finalname) throws Exception
	{
		Compressible comp = new Compressible();

		QuickMark qmark   = new QuickMark();
		QuickMerge qmerge = new QuickMerge();

		PairMark pmark   = new PairMark();
		PairMerge pmerge = new PairMerge();

		int stage = 0;
		long compressible = 0;

		RunningJob job = null;

		if (BrushConfig.RESTART_COMPRESS > 0)
		{
			stage = BrushConfig.RESTART_COMPRESS;
			compressible = BrushConfig.RESTART_COMPRESS_REMAIN;

			msg("  Restarting compression after stage " + stage + ":");

			BrushConfig.RESTART_COMPRESS = 0;
			BrushConfig.RESTART_COMPRESS_REMAIN = 0;
		}
		else
		{
			// Mark compressible nodes
			start("  Compressible");
			job = comp.run(basePath+startname, basePath+startname+"."+stage);
			compressible = counter(job, "compressible");
			end(job);
		}

		msg("  " + compressible + " compressible\n");

		long lastremaining = compressible;
        long threshold = 10;
		while (lastremaining > 0)
		{
			int prev = stage;
			stage++;

			String input  = basePath + startname + "." + Integer.toString(prev);
			String input0 = input + ".0";
			String output = basePath + startname + "." + Integer.toString(stage);

			long remaining = 0;
			if (lastremaining < BrushConfig.HADOOP_LOCALNODES || threshold < 10 )
			{
				// Send all the compressible nodes to the same machine for serial processing
				start("  QMark " + stage);
				job = qmark.run(input, input0);
				end(job);

				msg("  " + counter(job, "compressibleneighborhood") + " marked\n");

				start("  QMerge " + stage);
				job = qmerge.run(input0, output);
				end(job);

				remaining = counter(job, "needcompress");
			}
			else
			{
				// Use the randomized algorithm
				double rand = Math.random();

				start("  Mark " + stage);
				job = pmark.run(input, input0, (int)(rand*10000000));
				end(job);

				msg("  " + counter(job, "mergestomake") + " marked\n");

				start("  Merge " + stage);
				job = pmerge.run(input0, output);
				end(job);

				remaining = counter(job,"needscompress");
			}

			cleanup(input);
			cleanup(input0);

			String percchange = df.format((lastremaining > 0) ? 100*(remaining - lastremaining) / lastremaining : 0);
			msg("  " + remaining + " remaining (" + percchange + "%)\n");
            threshold = (lastremaining > 0) ? 100*(lastremaining - remaining) / lastremaining : 0;
            lastremaining = remaining;
		}

		save_result(basePath, startname + "." + stage, finalname);
        return compressible;
	}

	// Maximally remove tips
	///////////////////////////////////////////////////////////////////////////

	public void removeTips(String basePath, String current, String prefix, String finalname) throws Exception
	{
		TipsRemoval tips = new TipsRemoval();
		int round = 0;
		long remaining = 1;

		if (BrushConfig.RESTART_TIP > 0)
		{
			round = BrushConfig.RESTART_TIP;
			BrushConfig.RESTART_TIP = 0;
		}

		while (remaining > 0)
		{
			round++;

			String output = prefix + "." + round;
			long removed = 0;

			if (BrushConfig.RESTART_TIP_REMAIN > 0)
			{
				remaining = BrushConfig.RESTART_TIP_REMAIN;
				BrushConfig.RESTART_TIP_REMAIN = 0;
				msg("Restart remove tips " + round + ":");
				removed = 123456789;
			}
			else
			{
				RunningJob job;
                start("\nRemove Tips " + round);
                job = tips.run(basePath+current, basePath+output);
				end(job);

				removed = counter(job, "tips_found");
				remaining = counter(job, "tips_kept");
			}

			msg("  " + removed + " tips found, " + remaining + " remaining\n");

			if (removed > 0)
			{
				if (round > 1) { cleanup(current); }

				current = output + ".cmp";
				compressChains(basePath, output, current);
				remaining = 1;
			}

			cleanup(output);
		}

		save_result(basePath, current, finalname);
		msg("\n");
	}
    
    // Maximally pop bubbles
	///////////////////////////////////////////////////////////////////////////

	public long popallbubbles(String basePath, String basename, String intermediate, String finalname) throws Exception
	{
		long allpopped = 0;
		long popped    = 1;
		int round      = 1;

		FindBubbles finder = new FindBubbles();
		PopBubbles  popper = new PopBubbles();

		while (popped > 0)
		{
			String findname = intermediate + "." + round + ".f";
			String popname  = intermediate + "." + round;
			String cmpname  = intermediate + "." + round + ".cmp";

			start("Find Bubbles " + round);
			RunningJob job = finder.run(basePath+basename, basePath+findname);
			end(job);

			long potential = counter(job, "potentialbubbles");
			msg("  " + potential + " potential bubbles\n");

			start("  Pop " + round);
			job = popper.run(basePath+findname, basePath+popname);
			end(job);

			popped = counter(job, "bubblespopped");
			msg("  " + popped + " bubbles popped\n");

			cleanup(findname);

			if (popped > 0)
			{
				if (round > 1)
				{
					cleanup(basename);
				}

				compressChains(basePath, popname, cmpname);

				basename = cmpname;
				allpopped += popped;
				round++;
			}

			cleanup(popname);
		}

		// Copy the basename to the final name
		save_result(basePath, basename, finalname);
		msg("\n");

		return allpopped;
	}


    // Maximally remove low coverage nodes & compress
	///////////////////////////////////////////////////////////////////////////
    
	public void removelowcov(String basePath, String nobubblescmp, String lowcov, String lowcovcmp) throws Exception
	{
        RunningJob job;
        RemoveLowCoverage rlc = new RemoveLowCoverage();
        //\\\\\\\\\\\\
        /*start("\n  Define Consensus");
        DefineConsensus dc = new DefineConsensus();
        job = dc.run(basePath + nobubblescmp, basePath + nobubblescmp + ".dc");
        end(job);
        start("\n  Count Braids");
        CountBraid cb = new CountBraid();
        job = cb.run(basePath + nobubblescmp + ".dc", basePath + nobubblescmp + ".cb");
        long nodes = counter(job, "nodes");
        long edges = counter(job, "edges");
        long braids = counter(job, "braids");
        msg("  " + braids + " braids  " + nodes + " nodes  " + edges + " edges");
        end(job);*/
        start("\n  Remove Low Coverage");
        job = rlc.run(basePath + nobubblescmp, basePath + lowcov);
        end(job);
        long lowcovremoved = counter(job, "lowcovremoved");
        msg("  " + lowcovremoved + " lowcovremoved\n");
        msg("\n\n");

		if (lowcovremoved > 0)
		{
			compressChains(basePath, lowcov, lowcov+".c");
			removeTips(basePath, lowcov+".c", lowcov+".t", lowcov+".tc");
			popallbubbles(basePath, lowcov+".tc", lowcov+".b", lowcovcmp);
		}
		else
		{
			save_result(basePath, lowcov, lowcovcmp);
		}
        //\\\\\\\\\\\\\\
        /*start("\n  Define Consensus");
        //DefineConsensus dc = new DefineConsensus();
        job = dc.run(basePath + lowcovcmp, basePath + lowcovcmp + ".dc");
        end(job);
        start("\n  Count Braids");
        //CountBraid cb = new CountBraid();
        job = cb.run(basePath + lowcovcmp + ".dc", basePath + lowcovcmp + ".cb");
        nodes = counter(job, "nodes");
        edges = counter(job, "edges");
        braids = counter(job, "braids");
        msg("  " + braids + " braids  " + nodes + " nodes  " + edges + " edges\n");
        end(job);
        msg("\n");*/
	}

    public void pairedgeAdjustment(String basePath, String current, String prefix, String finalname) throws Exception
	{
        CountReads cr = new CountReads();
        AdjustMateEdge ame = new AdjustMateEdge();
        EdgeRemoval re = new EdgeRemoval();
        RunningJob job;
        long cut_edge;
        long remaining = 1;
        long round = 0; 
		while (remaining > 0)
		{
			round++;
			String output = prefix + "." + round;
			long removed = 0;
            long edge_removal = 0;

			if (BrushConfig.RESTART_TIP_REMAIN > 0)
			{
				remaining = BrushConfig.RESTART_TIP_REMAIN;
				BrushConfig.RESTART_TIP_REMAIN = 0;
				msg("Restart remove tips " + round + ":");
				removed = 123456789;
			}
			else
			{
                start("\nCount Reads ");
                job = cr.run(basePath + current, basePath + prefix+".count");
                end(job);
                long reads = counter(job, "reads");
                long ctg_sum = counter(job,"ctg_sum");
                start("\n  Pair Edge Adjustment  " + round);
                job = ame.run(basePath + current, basePath + output + ".ame", reads, ctg_sum);
                end(job);
                long repeat =  counter(job, "repeat");
                long unique = counter(job, "unique");
                long edges_5 = counter(job, "5_edges");
                long edges_10 = counter(job, "10_edges");
                long edges_05 = counter(job, "05_edges");
                long edges_01 = counter(job, "01_edges");
                long edges = counter(job, "edges");
                edge_removal = counter(job, "edge_removal");
                msg("  " + edges + " edge " + edge_removal + " edge_removal " + edges_5 + " edge_5 " + edges_10 + " edges_10 " + edges_05 + " edges_05 " + edges_01 + " edges_01" );
                msg("  " + unique + " unique " + repeat + " repeat " );   
                //\\
                if (round > 1 && edge_removal == 0) {
                    break;
                }
                //\\
                start("\n  Remove Edges");
                re = new EdgeRemoval();
                job = re.run(basePath + output + ".ame", basePath + output + ".re");
                end(job);
                msg("\n");
                cut_edge = edge_removal;
                current = output + ".re";
				remaining = 0;
			}
            
			if (edge_removal > 0)
			{
				if (round > 1) { cleanup(current); }
				current = output + ".cmp";
				compressChains(basePath, output + ".re", current);
				remaining = 1;
			}

			cleanup(output);
		}
        save_result(basePath, current, finalname);
		msg("\n");
    }
    
    
    
    // Run an entire assembly
	///////////////////////////////////////////////////////////////////////////
    public int run(String[] args) throws Exception
	{
        BrushConfig.parseOptions(args);
	    BrushConfig.validateConfiguration();

		// Setup to use a file appender
	    BasicConfigurator.resetConfiguration();

		TTCCLayout lay = new TTCCLayout();
		lay.setDateFormat("yyyy-mm-dd HH:mm:ss.SSS");

	    FileAppender fa = new FileAppender(lay, BrushConfig.localBasePath+"brush.details.log", true);
	    fa.setName("File Appender");
	    fa.setThreshold(Level.INFO);
	    BasicConfigurator.configure(fa);

	    logfile = new FileOutputStream(BrushConfig.localBasePath+"brush.log", true);
	    logstream = new PrintStream(logfile);

		BrushConfig.printConfiguration();

		// Time stamp
		DateFormat dfm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		msg("== Starting time " + dfm.format(new Date()) + "\n");
		long globalstarttime = System.currentTimeMillis();
        long GCstarttime=0;
        long GCendtime=0;
        long GSstarttime=0;
        long GSendtime=0;
        long GC2starttime=0;
        long GC2endtime=0;
        long GS2starttime=0;
        long GS2endtime=0;
        long FPstarttime=0;
        long FPendtime=0;
        
		if (BrushConfig.RUN_STATS != null)
		{
			computeStats("", BrushConfig.RUN_STATS);
		}
		else if (BrushConfig.CONVERT_FA != null)
		{
			convertFasta("", BrushConfig.CONVERT_FA, BrushConfig.CONVERT_FA + ".fa");
		}
		else
		{
			// Assembly Pipeline
            // PreProcess - contained reads, count kmer coverage
            if (runStage("preprocess"))
			{
                GCstarttime = System.currentTimeMillis();
				preprocess(BrushConfig.hadoopReadPath, BrushConfig.hadoopBasePath, preprocess);
				checkDone();
			}
            // Build overlap graph
            if (runStage("buildOverlap"))
			{
				buildOverlap(BrushConfig.hadoopReadPath, BrushConfig.hadoopBasePath, preprocess, overlap);
				computeStats(BrushConfig.hadoopBasePath, overlap);
				checkDone();
			}
            // Build string graph
            if (runStage("buildString"))
			{
				buildStringGraph(BrushConfig.hadoopReadPath, BrushConfig.hadoopBasePath, overlap, string);
                GCendtime = System.currentTimeMillis();
				computeStats(BrushConfig.hadoopBasePath, string);
				checkDone();
			}

			if (runStage("removeTips"))
			{
                GSstarttime = System.currentTimeMillis();
				removeTips(BrushConfig.hadoopBasePath, string, notips, notipscmp);
				computeStats(BrushConfig.hadoopBasePath, notipscmp);
				checkDone();
			}

            if (runStage("popBubbles"))
			{
				popallbubbles(BrushConfig.hadoopBasePath, notipscmp, nobubbles, nobubblescmp);
				computeStats(BrushConfig.hadoopBasePath, nobubblescmp);
				checkDone();
			} 
  
            if (runStage("lowcov"))
			{
				removelowcov(BrushConfig.hadoopBasePath, nobubblescmp, lowcov, lowcovcmp);
                computeStats(BrushConfig.hadoopBasePath, lowcovcmp);
				checkDone();
			}
            
            /*if (runStage("adjustpairedges"))
			{
                GC2starttime = System.currentTimeMillis();
				pairedgeAdjustment(BrushConfig.hadoopBasePath, lowcovcmp, pairadjust, pairadjustcmp);
				computeStats(BrushConfig.hadoopBasePath, pairadjustcmp);
				checkDone();
			}*/
            
            if (runStage("adjustedges"))
			{
				edgeAdjustment(BrushConfig.hadoopBasePath, lowcovcmp, edgeadjust, edgeadjustcmp);
                GSendtime = System.currentTimeMillis();
				computeStats(BrushConfig.hadoopBasePath, edgeadjustcmp);
				checkDone();
			}
            
			if (runStage("convertFasta"))
			{
				convertFasta(BrushConfig.hadoopBasePath, edgeadjustcmp, finalcmp + ".fa");
				checkDone();
			}
		}

        // Final timestamp
		long globalendtime = System.currentTimeMillis();
		long globalduration = (globalendtime - globalstarttime)/1000;
        long gcduration = (GCendtime - GCstarttime)/1000;
        long gsduration = (GSendtime - GSstarttime)/1000;
        //long gc2duration = (GC2endtime - GC2starttime)/1000;
        long fpduration = (FPendtime - FPstarttime)/1000;
		msg("== Ending time " + dfm.format(new Date()) + "\n");
		msg("== Duration: " + globalduration + " s, " + GLOBALNUMSTEPS + " total steps\n");
        msg(gcduration + " s, Graph Construction\n");
        msg(gsduration + " s, Graph Simplification\n");
        //msg(gc2duration + " s, Graph Construction2\n");
		return 0;
	}

    public static void main(String[] args) throws Exception
	{
		int res = ToolRunner.run(new Configuration(), new BrushAssembler(), args);
		System.exit(res);
	}
}
