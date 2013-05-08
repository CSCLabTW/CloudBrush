/*
    BrushConfig.java
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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.mapred.JobConf;

public class BrushConfig {
    // important paths
	public static String hadoopReadPath = null;
	public static String hadoopBasePath = null;
	public static String hadoopTmpPath = null;
	public static String localBasePath = "work";

	// hadoop options
	public static int    HADOOP_MAPPERS    = 50;
	public static int    HADOOP_REDUCERS   = 50;
	public static int    HADOOP_LOCALNODES = 1000;
	public static long   HADOOP_TIMEOUT    = 0;
	public static String HADOOP_JAVAOPTS   = "-Xmx4000m";

	// Assembler options
	public static String STARTSTAGE = null;
	public static String STOPSTAGE = null;

    // restart options
	public static boolean validateonly = false;
	public static boolean forcego = false;
	public static int RESTART_INITIAL = 0;
	public static int RESTART_TIP = 0;
	public static int RESTART_TIP_REMAIN = 0;
	public static int RESTART_COMPRESS = 0;
	public static int RESTART_COMPRESS_REMAIN = 0;

    // initial node construction
	public static long K = -1;
    public static long READLEN = -1;
    public static long INSLEN = 200;
    public static long INSLEN_SD = 20;
    public static float EXPCOV = 50;
    public static float KMERCOV = 30;
    public static float MAJORITY = 0.6f;
    public static float PWM_N = 0.1f;

    // error
    public static float ERRORRATE = 0.00f;

    // tips
	public static long TIPLENGTH = -3;

    // bubbles
	public static long MAXBUBBLELEN    = -5;
	public static float BUBBLEEDITRATE = 0.05f;

    // low cov
	public static float LOW_COV_THRESH  = -1.0f;
	public static long  MAX_LOW_COV_LEN = -2;

    // kmer status
    public static long LOW_KMER = 1;
    public static long UP_KMER = 2000;

    // stats
	public static String RUN_STATS = null;
	public static long  N50_TARGET = -1;
	public static String CONVERT_FA = null;

    public static void validateConfiguration()
	{
        if (TIPLENGTH < 0)         { TIPLENGTH = 10*READLEN; }
        if (MAXBUBBLELEN < 0)      { MAXBUBBLELEN = 4*READLEN-2*K-1; }
        if (MAX_LOW_COV_LEN < 0)   { MAX_LOW_COV_LEN = 2*READLEN; }
        if (LOW_COV_THRESH < 0)   { LOW_COV_THRESH = 1; }
        int err = 0;
        if ((RUN_STATS == null) && (CONVERT_FA == null)) {
            if (hadoopBasePath == null) { err++; System.err.println("ERROR: -asm is required"); }
            if (STARTSTAGE == null && hadoopReadPath == null) { err++; System.err.println("ERROR: -reads is required"); }
            if (READLEN <= 0)                 { err++; System.err.println("ERROR: -readlen is required"); }
            if (K <= 0)                 { err++; System.err.println("ERROR: -k is required"); }
        }
        if (err > 0) { System.exit(1); }
        if (!hadoopBasePath.endsWith("/")) { hadoopBasePath += "/"; }
		if (!localBasePath.endsWith("/"))  { localBasePath += "/"; }
		hadoopTmpPath = hadoopBasePath.substring(0, hadoopBasePath.length()-1) + ".tmp" + "/";
	}

    public static void initializeConfiguration(JobConf conf)
	{
		validateConfiguration();

        conf.setNumMapTasks(HADOOP_MAPPERS);
		conf.setNumReduceTasks(HADOOP_REDUCERS);
		conf.set("mapred.child.java.opts", HADOOP_JAVAOPTS);
		conf.set("mapred.task.timeout", Long.toString(HADOOP_TIMEOUT));
		conf.setLong("LOCALNODES", HADOOP_LOCALNODES);


        conf.setLong("UP_KMER", UP_KMER);
        conf.setLong("LOW_KMER", LOW_KMER);
        conf.setLong("K", K);
        //conf.setFloat("ERRORRATE", ERRORRATE);
        conf.setFloat("MAJORITY", MAJORITY);
        conf.setFloat("PWM_N", PWM_N);
        conf.setFloat("EXPCOV", EXPCOV);
        conf.setFloat("KMERCOV", KMERCOV);
        conf.setLong("READLENGTH", READLEN);
        conf.setLong("TIPLENGTH", TIPLENGTH);
        conf.setLong("INSLENGTH", INSLEN);
        conf.setLong("INSLENGTH_SD", INSLEN_SD);
		conf.setLong("MAXBUBBLELEN", MAXBUBBLELEN);
        conf.setFloat("BUBBLEEDITRATE", BUBBLEEDITRATE);
        conf.setFloat("LOW_COV_THRESH", LOW_COV_THRESH);
		conf.setLong("MAX_LOW_COV_LEN", MAX_LOW_COV_LEN);
        //conf.setFloat("ERRORRATE", ERRORRATE);

        conf.setLong("N50_TARGET", N50_TARGET);
	}

    public static void printConfiguration()
	{
		validateConfiguration();

		//BrushAssembler.msg("Contrail " + Contrail.VERSION + "\n");
		BrushAssembler.msg("==================================================================================\n");
		BrushAssembler.msg("Input: "         + hadoopReadPath + "\n");
		BrushAssembler.msg("Workdir: "       + hadoopBasePath  + "\n");
		BrushAssembler.msg("localBasePath: " + localBasePath + "\n");

		BrushAssembler.msg("HADOOP_MAPPERS = "    + HADOOP_MAPPERS + "\n");
		BrushAssembler.msg("HADOOP_REDUCERS = "   + HADOOP_REDUCERS + "\n");
		BrushAssembler.msg("HADOOP_JAVA_OPTS = "  + HADOOP_JAVAOPTS + "\n");
		BrushAssembler.msg("HADOOP_TIMEOUT = "    + HADOOP_TIMEOUT + "\n");
		BrushAssembler.msg("HADOOP_LOCALNODES = " + HADOOP_LOCALNODES + "\n");

		if (STARTSTAGE != null)  { BrushAssembler.msg("STARTSTAGE = " + STARTSTAGE + "\n"); }

		//BrushAssembler.msg("RESTART_INITIAL = " + RESTART_INITIAL + "\n");

		//BrushAssembler.msg("RESTART_TIP = " + RESTART_TIP + "\n");
		//BrushAssembler.msg("RESTART_TIP_REMAIN = " + RESTART_TIP_REMAIN + "\n");

		//BrushAssembler.msg("RESTART_COMPRESS = " + RESTART_COMPRESS + "\n");
		//BrushAssembler.msg("RESTART_COMPRESS_REMAIN = " + RESTART_COMPRESS_REMAIN + "\n");

		//BrushAssembler.msg("RESTART_SCAFF_PHASE = " + RESTART_SCAFF_PHASE + "\n");
		//BrushAssembler.msg("RESTART_SCAFF_STAGE = " + RESTART_SCAFF_STAGE + "\n");
		//BrushAssembler.msg("RESTART_SCAFF_FRONTIER = " + RESTART_SCAFF_FRONTIER + "\n");

		if (STOPSTAGE  != null) { BrushAssembler.msg("STOPSTAGE = "  + STOPSTAGE + "\n");  }
        BrushAssembler.msg("READLENGTH = "               + READLEN + "\n");
		BrushAssembler.msg("K = "               + K + "\n");
        //BrushAssembler.msg("INSERTLENGTH = "               + INSLEN + "\n");
		//BrushAssembler.msg("INSERTLENGTH_SD = "               + INSLEN_SD + "\n");
        //BrushAssembler.msg("EXPCOV = "               + EXPCOV + "\n");
        BrushAssembler.msg("KMERCOV = "               + KMERCOV + "\n");
        //BrushAssembler.msg("ERRORRATE = "               + ERRORRATE + "\n");
        BrushAssembler.msg("PWM MAJORITY = "               + MAJORITY + "\n");
        BrushAssembler.msg("PWM N = "               + PWM_N + "\n");
        BrushAssembler.msg("KMER UPPER BOUND = "    + UP_KMER + "\n");
		BrushAssembler.msg("KMER LOW BOUND = "  + LOW_KMER + "\n");
		BrushAssembler.msg("TIPLENGTH = "       + TIPLENGTH + "\n");
		BrushAssembler.msg("MAXBUBBLELEN = "    + MAXBUBBLELEN + "\n");
		BrushAssembler.msg("BUBBLEEDITRATE = "  + BUBBLEEDITRATE + "\n");
		BrushAssembler.msg("LOW_COV_THRESH = "  + LOW_COV_THRESH + "\n");
	    BrushAssembler.msg("MAX_LOW_COV_LEN = " + MAX_LOW_COV_LEN + "\n");

		//BrushAssembler.msg("MIN_THREAD_WEIGHT = " + MIN_THREAD_WEIGHT + "\n");

		//BrushAssembler.msg("INSERT_LEN = "     + INSERT_LEN + "\n");
		//BrushAssembler.msg("MIN_WIGGLE = "     + MIN_WIGGLE + "\n");
		//BrushAssembler.msg("MIN_CTG_LEN = "    + MIN_CTG_LEN + "\n");
		//BrushAssembler.msg("MIN_UNIQUE_COV = " + MIN_UNIQUE_COV + "\n");
		//BrushAssembler.msg("MAX_UNIQUE_COV = " + MAX_UNIQUE_COV + "\n");
		//BrushAssembler.msg("MAX_FRONTIER = "   + MAX_FRONTIER + "\n");

		BrushAssembler.msg("RUN_STATS = " + RUN_STATS + "\n");
		BrushAssembler.msg("N50_TARGET = " + N50_TARGET + "\n");

		BrushAssembler.msg("CONVERT_FA = " + CONVERT_FA + "\n");

		BrushAssembler.msg("\n");

		if (validateonly && !forcego)
		{
			System.exit(0);
		}
	}

    public static void parseOptions(String [] args)
	{
		Options options = new Options();

        options.addOption(new Option("help",     "print this message"));
		options.addOption(new Option("h",        "print this message"));
		options.addOption(new Option("expert",   "show expert options"));
		//options.addOption(new Option("validate", "validate and print options"));
		//options.addOption(new Option("go",       "go even when validating"));
        
        // work directories
		options.addOption(OptionBuilder.withArgName("hadoopBasePath").hasArg().withDescription("Base Hadoop assembly directory [required]").create("asm"));
		options.addOption(OptionBuilder.withArgName("hadoopReadPath").hasArg().withDescription("Hadoop read directory [required]").create("reads"));
		options.addOption(OptionBuilder.withArgName("workdir").hasArg().withDescription("Local work directory (default: " + localBasePath + ")").create("work"));

        // hadoop options
		options.addOption(OptionBuilder.withArgName("numSlots").hasArg().withDescription("Number of machine slots to use (default: " + HADOOP_MAPPERS + ")").create("slots"));
		options.addOption(OptionBuilder.withArgName("numNodes").hasArg().withDescription("Max nodes in memory (default: " + HADOOP_LOCALNODES + ")").create("nodes"));
		options.addOption(OptionBuilder.withArgName("childOpts").hasArg().withDescription("Child Java Options (default: " + HADOOP_JAVAOPTS + ")").create("javaopts"));
		options.addOption(OptionBuilder.withArgName("millisecs").hasArg().withDescription("Hadoop task timeout (default: " + HADOOP_TIMEOUT + ")").create("timeout"));

        // job restart
		options.addOption(OptionBuilder.withArgName("stage").hasArg().withDescription("Starting stage").create("start"));
		options.addOption(OptionBuilder.withArgName("stage").hasArg().withDescription("Stop stage").create("stop"));
/*		
		options.addOption(new Option("restart_initial", "restart after build initial, before quickmerge"));
		
		options.addOption(OptionBuilder.withArgName("stage").hasArg().withDescription("Last completed compression stage [expert]").create("restart_compress"));
		options.addOption(OptionBuilder.withArgName("cnt").hasArg().withDescription("Number remaining nodes [expert]").create("restart_compress_remain"));
		
		options.addOption(OptionBuilder.withArgName("stage").hasArg().withDescription("Last completed tip removal [expert]").create("restart_tip"));
		options.addOption(OptionBuilder.withArgName("cnt").hasArg().withDescription("Number remaining tips [expert]").create("restart_tip_remain"));
		
		
		options.addOption(OptionBuilder.withArgName("phase").hasArg().withDescription("last completed phase").create("restart_scaff_phase"));
		options.addOption(OptionBuilder.withArgName("stage").hasArg().withDescription("restart at this stage").create("restart_scaff_stage"));
		options.addOption(OptionBuilder.withArgName("hops").hasArg().withDescription("last completed frontier hop").create("restart_scaff_frontier"));
*/		
        // initial graph
        options.addOption(OptionBuilder.withArgName("read length").hasArg().withDescription("Read Length ").create("readlen"));
        //options.addOption(OptionBuilder.withArgName("expcov").hasArg().withDescription("Expected Coverage ").create("expcov"));
        options.addOption(OptionBuilder.withArgName("kmercov").hasArg().withDescription("Kmer Coverage ").create("kmercov"));
		options.addOption(OptionBuilder.withArgName("k").hasArg().withDescription("Graph nodes size [required]").create("k"));
        //options.addOption(OptionBuilder.withArgName("insert length").hasArg().withDescription("Insert Length ").create("inslen"));
        //options.addOption(OptionBuilder.withArgName("insert length sd").hasArg().withDescription("Insert Length Stdev ").create("inslen_sd"));

        // kmer status
        options.addOption(OptionBuilder.withArgName("kmer upper bound").hasArg().withDescription("max kmer cov (default: " + UP_KMER ).create("kmerup"));
		options.addOption(OptionBuilder.withArgName("kmer lower bound").hasArg().withDescription("min kmer cov (default: " + LOW_KMER).create("kmerlow"));
     
        // error correction
		options.addOption(OptionBuilder.withArgName("tip bp").hasArg().withDescription("max tip trim length (default: " + -TIPLENGTH +"K)").create("tiplen"));
		options.addOption(OptionBuilder.withArgName("bubble bp").hasArg().withDescription("max bubble length (default: " + -MAXBUBBLELEN + "K)").create("bubblelen"));
		options.addOption(OptionBuilder.withArgName("bubble erate").hasArg().withDescription("max bubble error rate (default: " + BUBBLEEDITRATE + ")").create("bubbleerate"));
        options.addOption(OptionBuilder.withArgName("PWM majority").hasArg().withDescription("majority (default: " + MAJORITY + ")").create("maj"));
        options.addOption(OptionBuilder.withArgName("PWM N's threshold").hasArg().withDescription("PWM N (default: " + PWM_N + ")").create("N"));
        options.addOption(OptionBuilder.withArgName("min cov").hasArg().withDescription("cut nodes below min cov (default: " + LOW_COV_THRESH +")").create("lowcov"));
		options.addOption(OptionBuilder.withArgName("max bp").hasArg().withDescription("longest nodes to cut (default: " + -MAX_LOW_COV_LEN +"K)").create("lowcovlen"));


        //stats
		options.addOption(OptionBuilder.withArgName("dir").hasArg().withDescription("compute stats").create("run_stats"));
		options.addOption(OptionBuilder.withArgName("genome bp").hasArg().withDescription("genome size for N50").create("genome"));
		
		//convert
		options.addOption(OptionBuilder.withArgName("dir").hasArg().withDescription("convert fa").create("convert_fa"));
        
        CommandLineParser parser = new GnuParser();

	    try
	    {
            CommandLine line = parser.parse( options, args );
            
            if (line.hasOption("help") || line.hasOption("h") || line.hasOption("expert"))
	        {
	        	System.out.print( "Usage: CloudBrush [-asm dir] [-reads dir] [-readlen readlen] [-k k] [options]\n" +
	        			         "\n" +
	        			         "CloudBrush Stages:\n" +
	        	                 "================\n" +
                                 "preprocess : read filter, contained reads, compute kmer coverage\n" +
	        	                 "\n" +
	        	                 "buildOverlap : build overlap graph\n" +
                                 "  -readlen <bp>       : Read length [required]\n" +
	        	                 "  -k <bp>             : Minimun overlap length [required]\n" +
                                 "  -kmerup <coverage>  : Kmer coverage upper bound [200]\n" +
                                 "  -kmerlow <coveage>  : Kmer coverage lower bound [1]\n" +
	        	                 "\n" +
                                 "buildString : edge adjustment and build string graph\n" +
                                 "  -maj <ratio>    : Majority of Position Weight Matrix [0.6f]\n" +
	        	                 "  -N <ratio>    :  Ratio of N character in consensus sequence [0.1f]\n" +
	        	                 "\n" +
	    		                 "removeTips : remove deadend tips\n" +
	    		                 "  -tiplen <len>       : Max tip length [10*readlen]\n" +
	    		                 "\n" +
	    		                 "popBubbles : pop bubbles\n" +
	    		                 "  -bubblelen <len>    : Max Bubble length [4*readlen-2*k-1]\n" +
	    		                 "  -bubbleerrate <len> : Max Bubble Error Rate [" + BUBBLEEDITRATE + "]\n" +
	    		                 "\n" +
	    		                 "lowcov : cut low coverage nodes\n" +
	    		                 "  -lowcov <coverage>    : Cut coverage threshold [1f]\n" +
	    		                 "  -lowcovlen <len>    : Cut length threshold [2*readlen]\n" +
	    		                 "\n" +
	    		                 "adjustedges : Edge adjustment\n" +
                                 "  -maj <ratio>    : Majority of Position Weight Matrix [0.6f]\n" +
                                 "  -N <ratio>    :  Ratio of N character in consensus sequence [0.1f]\n" +
	    		                 "  -kmercov <coverage>    : Expected Kmer coverage [required]\n" +
                                 "\n" +
	    		                 "convertFasta : convert final assembly to fasta format\n" +
	    		                 "  -genome <len>       : Genome size for N50 computation\n" +
	    		                 "\n" +
	    		                 "General Options:\n" +
	    		                 "===============\n" +
	    		                 "  -asm <asmdir>       : Hadoop Base directory for assembly [required]\n" +
	    		                 "  -reads <readsdir>   : Directory with reads [required]\n" + 
	    		                 "  -work <workdir>     : Local directory for output files [" + localBasePath + "]\n" +
	    		                 "  -slots <slots>      : Hadoop Slots to use [" + HADOOP_MAPPERS + "]\n" +
	        	                 "  -expert             : Show expert options\n");
	        	
	        	
	        	if (line.hasOption("expert"))
	        	{
	        	System.out.print("\n" +
	        			         "General Options:\n" +
	        			         "================\n" +
	    		                 "  -run_stats <dir>    : Just compute size stats of <dir>\n" +
	    		                 "  -convert_fa <dir>   : Just convert <dir> to fasta\n" +
	    		                 //"  -record_all_threads : Record threads on non-branching nodes\n" +
	    		                 "\n" +
	        			         "Hadoop Options:\n" +
	        			         "===============\n" +
	    		                 "  -nodes <max>        : Max nodes in memory [" + HADOOP_LOCALNODES + "]\n" +
	    		                 "  -javaopts <opts>    : Hadoop Java Opts [" + HADOOP_JAVAOPTS + "]\n" +
	    		                 "  -timeout <usec>     : Hadoop task timeout [" + HADOOP_TIMEOUT + "]\n" +
	    		                 "  -validate           : Just validate options\n" +
	    		                 "  -go                 : Execute even when validating\n" +
	        			         "\n" +
	    		                 "Stage management\n" +
	    		                 "================\n" +
	    		                 "  -start <stage>      : Start stage\n" +
	    		                 "  -stop  <stage>      : Stop stage\n" /*+
	    		                 "  -restart_initial               : Restart after build initial, before quickmerge\n" +
	    		                 "  -restart_compress <stage>      : Restart compress after this completed stage\n" +
	    		                 "  -restart_compress_remain <cnt> : Restart compress with these remaining\n" +
	    		                 "  -restart_tip <stage>           : Restart tips after this completed tips\n" +
	    		                 "  -restart_tip_remain <cnt>      : Restart tips with these remaining\n"*/
	        					);
	        	}
	        	
	        	System.exit(0);
	        }
            if (line.hasOption("validate")) { validateonly = true; }
	        if (line.hasOption("go"))       { forcego = true; }
            
            if (line.hasOption("asm"))   { hadoopBasePath = line.getOptionValue("asm");  }
            if (line.hasOption("reads")) { hadoopReadPath = line.getOptionValue("reads"); }
            if (line.hasOption("work"))  { localBasePath  = line.getOptionValue("work"); }
            if (line.hasOption("slots"))    { HADOOP_MAPPERS  = Integer.parseInt(line.getOptionValue("slots")); HADOOP_REDUCERS = HADOOP_MAPPERS; }
	        if (line.hasOption("nodes"))    { HADOOP_LOCALNODES      = Integer.parseInt(line.getOptionValue("nodes")); }
	        if (line.hasOption("javaopts")) { HADOOP_JAVAOPTS = line.getOptionValue("javaopts"); }
	        if (line.hasOption("timeout"))  { HADOOP_TIMEOUT  = Long.parseLong(line.getOptionValue("timeout")); }

            if (line.hasOption("start")) { STARTSTAGE = line.getOptionValue("start"); }
	        if (line.hasOption("stop"))  { STOPSTAGE  = line.getOptionValue("stop");  }
	        
	        if (line.hasOption("restart_initial"))    { RESTART_INITIAL    = 1; }
	        if (line.hasOption("restart_tip"))        { RESTART_TIP        = Integer.parseInt(line.getOptionValue("restart_tip")); }
	        if (line.hasOption("restart_tip_remain")) { RESTART_TIP_REMAIN = Integer.parseInt(line.getOptionValue("restart_tip_remain")); }

	        if (line.hasOption("restart_compress"))        { RESTART_COMPRESS        = Integer.parseInt(line.getOptionValue("restart_compress")); }
	        if (line.hasOption("restart_compress_remain")) { RESTART_COMPRESS_REMAIN = Integer.parseInt(line.getOptionValue("restart_compress_remain")); }
	      
            
            if (line.hasOption("readlen"))     { READLEN     = Long.parseLong(line.getOptionValue("readlen")); }
            if (line.hasOption("expcov"))     { EXPCOV     = Float.parseFloat(line.getOptionValue("expcov")); }
            if (line.hasOption("kmercov"))     { KMERCOV     = Float.parseFloat(line.getOptionValue("kmercov")); }
            if (line.hasOption("k"))     { K     = Long.parseLong(line.getOptionValue("k")); }
            if (line.hasOption("inslen"))     { INSLEN     = Long.parseLong(line.getOptionValue("inslen")); }
            if (line.hasOption("inslen_sd"))     { INSLEN_SD     = Long.parseLong(line.getOptionValue("inslen_sd")); }
            if (line.hasOption("kmerup"))       { UP_KMER      = Long.parseLong(line.getOptionValue("kmerup")); }
	        if (line.hasOption("kmerlow"))    { LOW_KMER   = Long.parseLong(line.getOptionValue("kmerlow")); }
            if (line.hasOption("tiplen"))       { TIPLENGTH      = Long.parseLong(line.getOptionValue("tiplen")); }
	        if (line.hasOption("bubblelen"))    { MAXBUBBLELEN   = Long.parseLong(line.getOptionValue("bubblelen")); }
	        if (line.hasOption("bubbleerate"))  { BUBBLEEDITRATE = Float.parseFloat(line.getOptionValue("bubbleerate")); }
            if (line.hasOption("error"))  { ERRORRATE = Float.parseFloat(line.getOptionValue("error")); }
            if (line.hasOption("maj"))  { MAJORITY = Float.parseFloat(line.getOptionValue("maj")); }
            if (line.hasOption("N"))  { PWM_N = Float.parseFloat(line.getOptionValue("N")); }
            if (line.hasOption("lowcov"))       { LOW_COV_THRESH    = Float.parseFloat(line.getOptionValue("lowcov")); }
	        if (line.hasOption("lowcovlen"))    { MAX_LOW_COV_LEN   = Long.parseLong(line.getOptionValue("lowcovlen")); }
            if (line.hasOption("genome"))       { N50_TARGET = Long.parseLong(line.getOptionValue("genome")); }
            if (line.hasOption("run_stats"))    { RUN_STATS = line.getOptionValue("run_stats"); }
	        
	        if (line.hasOption("convert_fa"))   { CONVERT_FA = line.getOptionValue("convert_fa"); }
        }
	    catch( ParseException exp )
	    {
	        System.err.println( "Parsing failed.  Reason: " + exp.getMessage() );
	        System.exit(1);
	    }

    }

}
