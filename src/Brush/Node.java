/*
    Node.java
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

import java.io.*;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.Collections;
import java.util.Comparator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
/**
 *
 * @author rocky
 */
public class Node {
    public static final String NODEMSG           = "N";
    public static final String UPDATEMSG         = "U";
    public static final String HASUNIQUEP        = "P";
    public static final String TRIMMSG           = "T";
    public static final String KILLMSG           = "K";
	public static final String EXTRACOV          = "V";
	public static final String KILLLINKMSG       = "L";
    public static final String COMPRESSPAIR      = "C";
    public static final String BUBBLELINKMSG     = "B";

    public static final String SUFFIXMSG     = "S";
    public static final String OVALMSG     = "O";
    public static final String GRAYMSG    = "G";
    public static final String DARKMSG   = "D";
    public static final String ATTRIBUTE   = "I";

    // Node msg field codes
	public static final String STR      = "s";
	public static final String COVERAGE = "v";
    public static final String CANCOMPRESS = "c";
    public static final String POPBUBBLE   = "p";
    public static final String MERGE       = "m";
    public static final String MERTAG   = "t"; 
    public static final String CONTAINED   = "n";
    public static final String REPLACEMENT   = "r";
    public static final String MATE   = "f";
    public static final String REMOVEDGE   = "e";
    public static final String ADJUSTEDGE   = "a";
    public static final String MERGETIPS   = "x";
    public static final String GRAYEDGE = "g";
    public static final String BLACKEDGE = "b";
    public static final String WHITEDGE = "w";
    public static final String FCOLOR = "0";
    public static final String RCOLOR = "1";
    public static final String UNIQUE = "u";

    //\\// for find path
    public static enum Color {
        W, G, B, F
    };

    static String [] dnachars  = {"A", "C", "G", "T"};
    static String [] edgetypes = {"ff", "fr", "rf", "rr"};
    static String [] dirs      = {"f", "r"};


    static Map<String, String> str2dna_ = initializeSTR2DNA();
	static Map<String, String> dna2str_ = initializeDNA2STR();

    // node members
    private String nodeid;
	private Map<String, List<String>> fields = new HashMap<String, List<String>>();

    // converts strings like A, GA, TAT, ACGT to compressed DNA codes (A,B,C,...,HA,HB)
	private static Map<String,String> initializeSTR2DNA()
	{
	   int num = 0;
	   int asciibase = 'A';

	   Map<String, String> retval = new HashMap<String, String>();

	   for (int xi = 0; xi < dnachars.length; xi++)
	   {
		   retval.put(dnachars[xi],
				      Character.toString((char) (num + asciibase)));

		   num++;

		   for (int yi = 0; yi < dnachars.length; yi++)
		   {
			   retval.put(dnachars[xi] + dnachars[yi],
					      Character.toString((char) (num + asciibase)));
			   num++;
		   }
	   }

	   for (int xi = 0; xi < dnachars.length; xi++)
	   {
		   for (int yi = 0; yi < dnachars.length; yi++)
		   {
			   String m = retval.get(dnachars[xi] + dnachars[yi]);

			   for (int zi = 0; zi < dnachars.length; zi++)
			   {
				   retval.put(dnachars[xi]+dnachars[yi]+dnachars[zi],
						      m + retval.get(dnachars[zi]));

				   for (int wi = 0; wi < dnachars.length; wi++)
				   {
					   retval.put(dnachars[xi]+dnachars[yi]+dnachars[zi]+dnachars[wi],
							      m+retval.get(dnachars[zi]+dnachars[wi]));
				   }
			   }
		   }
	   }

	   return retval;
	}

	// converts single letter dna codes (A,B,C,D,E...) to strings (A,AA,GT,GA...)
	private static Map<String, String> initializeDNA2STR()
	{
	   int num = 0;
	   int asciibase = 65;

	   Map<String, String> retval = new HashMap<String, String>();

	   for (int xi = 0; xi < dnachars.length; xi++)
	   {
		   retval.put(Character.toString((char) (num + asciibase)),
				      dnachars[xi]);

		   num++;

		   for (int yi = 0; yi < dnachars.length; yi++)
		   {
			   retval.put(Character.toString((char) (num + asciibase)),
					      dnachars[xi] + dnachars[yi]);
			   num++;
		   }
	   }

	   /*
	   Set<String> keys = retval.keySet();
	   Iterator<String> it = keys.iterator();
	   while(it.hasNext())
	   {
		   String k = it.next();
		   String v = retval.get(k);

		   System.err.println(k + "\t" + v);
	   }
	   */

	   return retval;
	}

	// converts a tight encoding to a normal ascii string
	public static String dna2str(String dna)
	{
		StringBuffer sb = new StringBuffer();

		for (int i = 0; i < dna.length(); i++)
		{
			sb.append(dna2str_.get(dna.substring(i,i+1)));
		}

		return sb.toString();
	}

	public static String str2dna(String seq)
	{
		StringBuffer sb = new StringBuffer();

	    int l = seq.length();

	    int offset = 0;

	    while (offset < l)
	    {
	    	int r = l - offset;

	    	if (r >= 4)
	    	{
	    		sb.append(str2dna_.get(seq.substring(offset, offset+4)));
	    		offset += 4;
	    	}
	    	else
	    	{
	    		sb.append(str2dna_.get(seq.substring(offset, offset+r)));
	    		offset += r;
	    	}
	    }

        return sb.toString();
	}

    //\\\\\\\\\
    public static int getLevenshteinDistance (String s, String t) {
      if (s == null || t == null) {
        throw new IllegalArgumentException("Strings must not be null");
      }
      /*
        The difference between this impl. and the previous is that, rather
         than creating and retaining a matrix of size s.length()+1 by t.length()+1,
         we maintain two single-dimensional arrays of length s.length()+1.  The first, d,
         is the 'current working' distance array that maintains the newest distance cost
         counts as we iterate through the characters of String s.  Each time we increment
         the index of String t we are comparing, d is copied to p, the second int[].  Doing so
         allows us to retain the previous cost counts as required by the algorithm (taking
         the minimum of the cost count to the left, up one, and diagonally up and to the left
         of the current cost count being calculated).  (Note that the arrays aren't really
         copied anymore, just switched...this is clearly much better than cloning an array
         or doing a System.arraycopy() each time  through the outer loop.)

         Effectively, the difference between the two implementations is this one does not
         cause an out of memory condition when calculating the LD over two very large strings.
      */

      int n = s.length(); // length of s
      int m = t.length(); // length of t

      if (n == 0) {
        return m;
      } else if (m == 0) {
        return n;
      }

      int p[] = new int[n+1]; //'previous' cost array, horizontally
      int d[] = new int[n+1]; // cost array, horizontally
      int _d[]; //placeholder to assist in swapping p and d

      // indexes into strings s and t
      int i; // iterates through s
      int j; // iterates through t

      char t_j; // jth character of t

      int cost; // cost

      for (i = 0; i<=n; i++) {
         p[i] = i;
      }

      for (j = 1; j<=m; j++) {
         t_j = t.charAt(j-1);
         d[0] = j;

         for (i=1; i<=n; i++) {
            cost = s.charAt(i-1)==t_j ? 0 : 1;
            // minimum of cell to the left+1, to the top+1, diagonally left and up +cost
            d[i] = Math.min(Math.min(d[i-1]+1, p[i]+1),  p[i-1]+cost);
         }

         // copy current distance counts to 'previous row' distance counts
         _d = p;
         p = d;
         d = _d;
      }

      // our last action in the above loop was to switch d and p, so p now
      // actually has the most recent cost counts
      return p[n];
    }
    //\\\\\\\\\
    
    //\\\\\\\\\\
    public static String matename(String readname)
	{
		if (readname.endsWith("_1"))
		{
			return readname.substring(0, readname.length()-2)+ "_2";
		}
        
        if (readname.endsWith("_2"))
		{
			return readname.substring(0, readname.length()-2)+ "_1";
		}
		
		return null;
	}
    //\\\\\\\\\\
    public static int Count_PairEnd(List<String> a_list, List<String> b_list){
        int sum = 0;
        if (a_list != null && b_list != null) {
            for(int i=0; i < a_list.size(); i++) {
                if (b_list.contains(matename(a_list.get(i)))) {
                    sum = sum + 1;
                }
            }
        }
        return sum;
    }
    
    private List<String> getOrAddField(String field)
	{
		if (fields.containsKey(field))
		{
			return fields.get(field);
		}

		List<String> retval = new ArrayList<String>();
		fields.put(field, retval);

		return retval;
	}

    public boolean hasMertag(String key)
	{
		return fields.containsKey(key);
	}

    public void setMertag(String tag)
	{
		List<String> l = getOrAddField(MERTAG);
		l.clear();
		l.add(tag);
    }

	public String getMertag() throws IOException
	{
		if (!fields.containsKey(MERTAG))
		{
			throw new IOException("Mertag not found: " + toNodeMsg());
		}

		return fields.get(MERTAG).get(0);
	}
    
    public void setCoverage(float cov)
	{
		List<String> l = getOrAddField(COVERAGE);
		l.clear();
		l.add(Float.toString(cov));
	}

    public void setReplacement(String replace)
	{
		List<String> l = getOrAddField(REPLACEMENT);
		l.clear();
		l.add(replace);
	}
    
    public String getReplacement()
	{
		if (fields.containsKey(REPLACEMENT))
		{
			return fields.get(REPLACEMENT).get(0);
		}

		return null;
	}
    
    public void setMerge(String dir)
	{
		List<String> l = getOrAddField(MERGE);
		l.clear();
		l.add(dir);
	}
    
    

	public String getMerge()
	{
		if (fields.containsKey(MERGE))
		{
			return fields.get(MERGE).get(0);
		}

		return null;
	}
    
    
    public boolean hasCustom(String key)
	{
		return fields.containsKey(key);
	}

	public void setCustom(String key, String value)
	{
		List<String> l = getOrAddField(key);
		l.clear();
		l.add(value);
	}

	public void addCustom(String key, String value)
	{
		List<String> l = getOrAddField(key);
		l.add(value);
	}

	public List<String> getCustom(String key)
	{
		return fields.get(key);
	}

    public void clearCustom(String key)
	{
		fields.remove(key);
	}

    public void addEdge(String et, String v)
	{
		List<String> l = getOrAddField(et);
		l.add(v);
	}

	public List<String> getEdges(String et) throws IOException
	{
		if (et.equals("ff") ||
		    et.equals("fr") ||
		    et.equals("rr") ||
		    et.equals("rf"))
		{
		  return fields.get(et);
		}

		throw new IOException("Unknown edge type: " + et);
	}

	public void setEdges(String et, List<String> edges)
	{
		if (edges == null || edges.size() == 0)
		{
			fields.remove(et);
		}
		else
		{
			fields.put(et, edges);
		}
	}

	public void clearEdges(String et)
	{
		fields.remove(et);
	}

    //\\ modify for overlap graph
    public boolean hasEdge(String et, String nid) throws IOException
	{
		List<String> edges = getEdges(et);
		if (edges == null) { return false; }

		for (String v : edges)
		{
			if (v.substring(0, v.indexOf("!")).equals(nid))
			{
				return true;
			}
		}

		return false;
	}

    public boolean hasEdge(String et, String nid, int oval_size) throws IOException
	{
		List<String> edges = getEdges(et);
		if (edges == null) { return false; }

		for (String v : edges)
		{
            String [] vals = v.split("!");
			if (vals[0].equals(nid) && Integer.parseInt(vals[1]) == oval_size )
			{
				return true;
			}
		}

		return false;
	}

    public boolean canCompress(String d)
	{
		if (fields.containsKey(CANCOMPRESS + d))
		{
			return fields.get(CANCOMPRESS + d).get(0).equals("1");
		}

		return false;
	}

	public void setCanCompress(String d, boolean can)
	{
		if (can)
		{
			List<String> l = getOrAddField(CANCOMPRESS + d);
			l.clear();
			l.add("1");
		}
		else
		{
			fields.remove(CANCOMPRESS+d);
		}
	}

    public boolean isUnique(){
        if (fields.containsKey(UNIQUE))
		{
			return fields.get(UNIQUE).get(0).equals("1");
		}

		return false;
    }

    public void setisUnique(boolean is){
        if (is)
		{
			List<String> l = getOrAddField(UNIQUE);
			l.clear();
			l.add("1");
		}
		else
		{
			fields.remove(UNIQUE);
		}
    }

    public void removelink(String id, String dir) throws IOException
	{
		boolean found = false;

		List<String> edges = getEdges(dir);

		if (edges != null)
		{
			for(int i = 0; i < edges.size(); i++)
			{
                int idx = edges.get(i).indexOf("!");
                if (idx == -1) {
                    throw new IOException("Error removing link from " + getNodeId() + ": Edge without overlap size information!! ");
                }
				if (edges.get(i).substring(0, idx).equals(id))
				{
					edges.remove(i);
					setEdges(dir, edges);
					found = true;
					break;
				}
			}
		}

        if (!found)
		{
			throw new IOException("Error removing link from " + getNodeId() + ": Can't find " + id + ":" + dir + "\n" + toNodeMsg());
		}
	}

    public void removelink(String id, String dir, int oval_size) throws IOException
	{
		boolean found = false;

		List<String> edges = getEdges(dir);

		if (edges != null)
		{
			for(int i = 0; i < edges.size(); i++)
			{
                String [] vals = edges.get(i).split("!");
                /*int idx = edges.get(i).indexOf("!");
                if (idx == -1) {
                    throw new IOException("Error removing link from " + getNodeId() + ": Edge without overlap size information!! ");
                }*/
				if (vals[0].equals(id) && Integer.parseInt(vals[1]) == oval_size)
				{
					edges.remove(i);
					setEdges(dir, edges);
					found = true;
					break;
				}
			}
		}

        if (!found)
		{
			throw new IOException("Error removing link from " + getNodeId() + ": Can't find " + id + ":" + dir + "\n" + toNodeMsg());
		}
	}
    
    public void removelink_update(String id, String dir, int oval_size) throws IOException
	{
		boolean found = false;

		List<String> edges = getEdges(dir);

		if (edges != null)
		{
			for(int i = 0; i < edges.size(); i++)
			{
                String [] vals = edges.get(i).split("!");
				if (vals[0].equals(id) && Integer.parseInt(vals[1]) == oval_size)
				{
					edges.remove(i);
					setEdges(dir, edges);
					found = true;
					break;
				}
			}
		}
	}

    public void replacelink_oval(String o, String ot, String n, String nt) throws IOException
	{
		//System.err.println(nodeid_m + " replacing " + o + ":" + ot + " => " + n + ":" + nt);

		boolean found = false;

		List<String> l = getOrAddField(ot);
		String temp = "";
        int idx =  0;
        String [] o_vals = o.split("!");
        String o_id = o_vals[0];
        int o_oval_size = Integer.parseInt(o_vals[1]);
        for (int li = l.size() - 1; li >= 0; li--)
		{

            String [] vals = l.get(li).split("!");
            String id = vals[0];
            int oval_size = Integer.parseInt(vals[1]);
            if ( id.equals(o_id) && oval_size == o_oval_size)
			{
				l.remove(li);
				found = true;
			}
            //temp = l.get(li);
            /*idx = temp.indexOf("!");
            if (idx == -1) {
                throw new IOException("Edge without overlap size information!! ");
            }
            if (temp.substring(0, idx).equals(o.substring(0,o.indexOf("!")) && ))
			{
				l.remove(li);
				found = true;
			}*/
		}

		if (!found)
		{
			throw new IOException(nodeid + " Couldn't find link " + o + " " + ot + " replace link " + n + " " + nt);
		}

		if (l.size() == 0)
		{
			fields.remove(ot);
		}

		l = getOrAddField(nt);
		l.add(n);

		/*if (fields.containsKey(THREAD))
		{
			l = getOrAddField(THREAD);

			for (int i = 0; i < l.size(); i++)
			{
				String thread = l.get(i);

				String [] vals = thread.split(":");

				if (vals[0].equals(ot) && vals[1].equals(o))
				{
					thread = nt + ":" + n + ":" + vals[2];
					l.set(i, thread);
				}
			}
		}*/
	}

    public void replacelink(String o, String ot, String n, String nt) throws IOException
	{
		//System.err.println(nodeid_m + " replacing " + o + ":" + ot + " => " + n + ":" + nt);

		boolean found = false;

		List<String> l = getOrAddField(ot);
		String temp = "";
        int idx =  0;
        String [] o_vals = o.split("!");
        String o_id = o_vals[0];
        int o_oval_size = Integer.parseInt(o_vals[1]);
        for (int li = l.size() - 1; li >= 0; li--)
		{

           /* String [] vals = l.get(li).split("!");
            String id = vals[0];
            int oval_size = Integer.parseInt(vals[1]);
            if ( id.equals(o_id) && oval_size == o_oval_size)
			{
				l.remove(li);
				found = true;
			}*/
            temp = l.get(li);
            idx = temp.indexOf("!");
            if (idx == -1) {
                throw new IOException("Edge without overlap size information!! ");
            }
            if (temp.substring(0, idx).equals(o.substring(0,o.indexOf("!"))))
			{
				l.remove(li);
				found = true;
			}
		}

		if (!found)
		{
			throw new IOException(nodeid + " Couldn't find link " + o + " " + ot + " replace link " + n + " " + nt);
		}

		if (l.size() == 0)
		{
			fields.remove(ot);
		}

		l = getOrAddField(nt);
		l.add(n);

	}

    public TailInfo getedge(String et, String id) throws IOException{
        List<String> edges = getEdges(et);
		if (edges == null) { return null; }
		for (String v : edges)
		{
			if (v.substring(0, v.indexOf("!")).equals(id))
			{
				TailInfo ti = new TailInfo();
                ti.dist = 1;
                int idx = v.indexOf("!");
                if ( idx != -1){
                    ti.id = v.substring(0, v.indexOf("!"));
                    ti.oval_size = Integer.parseInt(v.substring(idx+1));
                    ti.dir = et.substring(1);
                }
                return ti;
			}
		}
        return null;
    }

    public TailInfo gettail(String dir)
	{
		if (degree(dir) != 1 || getBlackEdges(dir).size() > 0)
		{
			return null;
		}

		TailInfo ti = new TailInfo();
		ti.dist = 1;

		String fd = dir + "f";
		if (fields.containsKey(fd))
		{
            String edge_field = fields.get(fd).get(0);
            int idx = edge_field.indexOf("!");
            if ( idx != -1){
                ti.id = edge_field.substring(0, edge_field.indexOf("!"));
                ti.oval_size = Integer.parseInt(edge_field.substring(idx+1));
            }
			//ti.id = fields.get(fd).get(0);
			ti.dir = "f";
		}

		fd = dir + "r";
		if (fields.containsKey(fd))
		{
            String edge_field = fields.get(fd).get(0);
            int idx = edge_field.indexOf("!");
            if ( idx != -1){
                ti.id = edge_field.substring(0, edge_field.indexOf("!"));
                ti.oval_size = Integer.parseInt(edge_field.substring(idx+1));
            }
			//ti.id = fields.get(fd).get(0);
			ti.dir = "r";
		}

		return ti;
	}

    //Accessors
	public String str()
	{
		return dna2str(fields.get(STR).get(0));
	}
	
	public String str_raw()
	{
		return fields.get(STR).get(0);
	}
	
	public void setstr_raw(String rawstr)
	{
		List<String> l = getOrAddField(STR);
		l.clear();		
		l.add(rawstr);
	}
	
	public void setstr(String str)
	{
		List<String> l = getOrAddField(STR);
		l.clear();
		l.add(Node.str2dna(str));
	}
	
	public int len()
	{
		return str().length();
	}
	
	public int degree(String dir)
	{
		int retval = 0;
		
		String fd = dir + "f";
		if (fields.containsKey(fd)) { retval += fields.get(fd).size(); }
		
		String rd = dir + "r";
		if (fields.containsKey(rd)) { retval += fields.get(rd).size(); }
		
		return retval;
	}
    
    public float cov()
	{
		return Float.parseFloat(fields.get(COVERAGE).get(0)); 
	}

    //\\// need modify
    public static String str_concat(String astr, String bstr, int K) throws IOException
	{
		//System.err.println("Str_len: " + astr.length() + " Oval_len:" + K + " astr:" + astr + " bstr:" + bstr);
		String as = astr.substring(astr.length()-K);
		String bs = bstr.substring(0, K);

		/*if (!as.equals(bs))
		{
			throw new IOException("as (" + as + ") != bs (" + bs + ")");
		}*/

		return astr + bstr.substring(K);
	}

    public Node(String n_id)
	{
		nodeid = n_id;
	}

	public Node()
	{

	}

    public void addBubble(String minor, String vmd, String vid, String umd, String uid, float extracov, int oval_size)
	{
		String msg = minor + "|" + vmd + "|" + vid + "|" + umd + "|" + uid + "|" + extracov + "|" + oval_size;

		List<String> l = getOrAddField(POPBUBBLE);
		l.add(msg);
	}

	public List<String> getBubbles()
	{
		if (fields.containsKey(POPBUBBLE))
		{
			return fields.get(POPBUBBLE);
		}

		return null;
	}

	public void clearBubbles()
	{
		fields.remove(POPBUBBLE);
	}

    public void addAdjustEdge(String node, String dir, String edge, int oval_size)
	{
		String msg = node + "|" + dir + "|" + edge + "|" + oval_size;

		List<String> l = getOrAddField(ADJUSTEDGE);
		l.add(msg);
	}

     public List<String> getAdjustEdges()
	{
		if (fields.containsKey(ADJUSTEDGE))
		{
			return fields.get(ADJUSTEDGE);
		}

		return null;
	}

     public void clearAdjustEdge()
	{
		fields.remove(ADJUSTEDGE);
	}


     public void addRemovalEdge(String node, String dir, String edge, int oval_size)
	{
		String msg = node + "|" + dir + "|" + edge + "|" + oval_size;

		List<String> l = getOrAddField(REMOVEDGE);
		l.add(msg);
	}

     public List<String> getRemovalEdges()
	{
		if (fields.containsKey(REMOVEDGE))
		{
			return fields.get(REMOVEDGE);
		}

		return null;
	}

     public void clearRemovalEdge()
	{
		fields.remove(REMOVEDGE);
	}

     public void addMergeTip(String node_id, float cov_sum, String tip_id)
    {
        String msg = node_id + "|" + cov_sum + "|" + tip_id;

		List<String> l = getOrAddField(MERGETIPS);
		l.add(msg);
    }

     public List<String> getMergeTips()
	{
		if (fields.containsKey(MERGETIPS))
		{
			return fields.get(MERGETIPS);
		}

		return null;
	}

     public void clearMergeTip()
	{
		fields.remove(MERGETIPS);
	}

    //\\// Finding path method
    public Color getColor(String adj){
        if (adj.equals("f")){
            return Color.valueOf(fields.get(FCOLOR).get(0));
        } else {
            return Color.valueOf(fields.get(RCOLOR).get(0));
        }
    }
  /*  public Color getFColor(){
        return Color.valueOf(fields.get(FCOLOR).get(0)); 
    }

    public Color getRColor(){
        return Color.valueOf(fields.get(RCOLOR).get(0));
    }*/

    public void setColor(Color color, String adj){
        List<String> l;
        if (adj.equals("f")){
            l = getOrAddField(FCOLOR);
        } else {
            l = getOrAddField(RCOLOR);
        }
        l.clear();
		l.add(color.toString());
    }

   /* public void setFColor(Color color){
        List<String> l = getOrAddField(FCOLOR);
		l.clear();
		l.add(color.toString());
    }
    
    public void setRColor(Color color) {
        List<String> l = getOrAddField(RCOLOR);
		l.clear();
		l.add(color.toString());
    }*/

    public void clearColor(String adj) {
        if (adj.equals("f")){
            fields.remove(FCOLOR);
        } else {
            fields.remove(RCOLOR);
        }
    }

   /* public void clearFColor() {
        fields.remove(FCOLOR);
    }

    public void clearRColor() {
        fields.remove(RCOLOR);
    }*/

    /*public void setGrayEdge() {

    }*/


    public void addGrayEdge(String con, String node_id)
	{
		String msg = con + "|" + node_id;

		List<String> l = getOrAddField(GRAYEDGE);
		l.add(msg);
	}

    public List<String> getGrayEdges()
	{
		if (fields.containsKey(GRAYEDGE))
		{
			return fields.get(GRAYEDGE);
		}

		return null;
	}

    public void clearGrayEdge() {
        fields.remove(GRAYEDGE);
    }

  /*  public boolean hasGrayEdge(String et, String nid) throws IOException
	{
		List<String> edges = getEdges(et);
		if (edges == null) { return false; }

		for (String v : edges)
		{
			if (v.substring(0, v.indexOf("!")).equals(nid))
			{
				return true;
			}
		}

		return false;
	}*/

    class BlackEdgeComparator implements Comparator {
        public int compare(Object element1, Object element2) {
            String obj1 = (String) element1;
            String obj2 = (String) element2;
            //con + "|" + node_id + "|" + str_raw + "|" + oval_size + "|" + cov + "|" + end;
            String[] val1 = obj1.split("\\|");
            String[] val2 = obj2.split("\\|");
            if ((int) ((Node.dna2str(val1[2]).length()-Integer.parseInt(val1[3])) - (Node.dna2str(val2[2]).length() - Integer.parseInt(val2[3])) ) >= 0) {
                return -1;
            } else {
                return 1;
            }
        }
    }

    class BlackEdgeComparator2 implements Comparator {
        public int compare(Object element1, Object element2) {
            String obj1 = (String) element1;
            String obj2 = (String) element2;
            //con + "|" + node_id + "|" + str_raw + "|" + oval_size + "|" + cov + "|" + end;
            String[] val1 = obj1.split("\\|");
            String[] val2 = obj2.split("\\|");
            if ((int) ((Node.dna2str(val1[2]).length()*Float.parseFloat(val1[4])) - (Node.dna2str(val2[2]).length()*Float.parseFloat(val2[4])) ) >= 0) {
                return -1;
            } else {
                return 1;
            }
        }
    }



    //\\\\\\\\\\\\\\\\\\\\\\\\\\\ String comparsion
    public static int _min2(int a, int b)
	{
		return (a<b) ? a : b;
	}

	public static int _max2(int a, int b)
	{
		return (a>b) ? a : b;
	}

	public static int _min3(int a, int b, int c)
	{
		  return a < b
		          ? a < c ? a : c
		          : b < c ? b : c;
	}

	public static int fastdistance(String word1, String word2)
	{
		int len1 = word1.length();
		int len2 = word2.length();

		int[][] d = new int[len1+1][len2+1];

		for (int i = 0; i <= len1; i++)
		{
			d[i][0] = i;
		}

		for (int j = 0; j <= len2; j++)
		{
			d[0][j] = j;
		}

		for (int i = 1; i <= len1; i++)
		{
			char w1 = word1.charAt(i-1);
			for (int j = 1; j <= len2; j++)
			{
				char w2 = word2.charAt(j-1);
				int e = (w1 == w2) ? 0 : 1;

				d[i][j] = _min3(d[i-1][j]+1, d[i][j-1]+1, d[i-1][j-1]+e);
			}
		}

		return d[len1][len2];
	}

    public static boolean fastcomparsion(String word1, String word2){
        boolean result = false;
        if (word1.length() < word2.length()){
            word2 = word2.substring(0, word1.length());
        } else {
            word1 = word1.substring(0, word2.length());
        }
        if (word1.length() > 1000){
            int interval = word1.length() / 100;
            int count = 0;
            for(int i = 0; i < 100; i++) {
                if (word1.substring(interval*i, interval*(i+1)).equals(word2.substring(interval*i, interval*(i+1)))) {
                    count = count + 1;
                }
            }
            if (word1.substring(interval*99).equals(word2.substring(interval*99))) {
                count = count + 1;
            }
          
            if (count >= 90) {
                result = true;
            }
        } else {
            int distance = fastdistance(word1, word2);
            float threshold = (float)distance / (float)word1.length();
            
            if (threshold < 0.1 ) {
                result = true;
            }
        }
        return result;
    }
    //\\\\\\\\\\\\\\\ String comparsion
   
    public String selectBlackEdge(String dir) {
        String best_edge = null;
        List<String> black_edges = getBlackEdges();
        if (black_edges != null) {
            Collections.sort(black_edges, new BlackEdgeComparator());
            //best_edge = black_edges.get(0);
            List<String> edge_list = new ArrayList<String>();
            for(int i=0; i < black_edges.size(); i++) {
                // con + "|" + node_id + "|" + str_raw + "|" + oval_size + "|" + cov
                String [] vals = black_edges.get(i).split("\\|");
                if (vals[0].equals(dir + "f") || vals[0].equals(dir +"r")){
                    //best_edge = black_edges.get(i);
                    //break;
                    String str = Node.dna2str(vals[2]);
                    if (vals[0].charAt(1) == 'r'){
                        str = Node.rc(str);
                    }
                    String str_cut = "";
                    str_cut = str.substring(Integer.parseInt(vals[3]));
                    edge_list.add(str_cut+"!"+vals[4]);
                }
            }
            //\\\\\\\\\\
            // Compute consensus sequence
            String consensus = "";
            if (edge_list.size() >= 2) {
                int consensus_len = edge_list.get(1).substring(0, edge_list.get(1).indexOf("!")).length();
                //\\ 0:A 1:T 2:C 3:G 4:Sum
                int[][] array = new int[consensus_len][5];
                for(int i=0; i < consensus_len; i++) {
                    for(int j=0; j < 5; j++) {
                        array[i][j] = 0;
                    }
                }
                for(int i=0; i < edge_list.size(); i++) {
                    String [] compare_edge = edge_list.get(i).split("!");
                    for(int j=0; j < compare_edge[0].length() && j < consensus_len; j++ ) {
                        array[j][4] = array[j][4] + (int)Float.parseFloat(compare_edge[1]);
                        if (compare_edge[0].charAt(j) == 'A'){
                            array[j][0] = array[j][0] + (int)Float.parseFloat(compare_edge[1]);
                        } else if (compare_edge[0].charAt(j) == 'T'){
                            array[j][1] = array[j][1] + (int)Float.parseFloat(compare_edge[1]);
                        } else if (compare_edge[0].charAt(j) == 'C'){
                            array[j][2] = array[j][2] + (int)Float.parseFloat(compare_edge[1]);
                        } else if (compare_edge[0].charAt(j) == 'G'){
                            array[j][3] = array[j][3] + (int)Float.parseFloat(compare_edge[1]);
                        }
                    }
                }
                // construct consensus
                int N_count = 0;
                int gap_count = 0;
                for(int i=0; i < array.length; i++){
                    if ((float)array[i][0]/(float)array[i][4] > 0.6) {
                        consensus = consensus + "A";
                        gap_count = 0;
                    } else if ((float)array[i][1]/(float)array[i][4] > 0.6) {
                        consensus = consensus + "T";
                        gap_count = 0;
                    } else if ((float)array[i][2]/(float)array[i][4] > 0.6) {
                        consensus = consensus + "C";
                        gap_count = 0;
                    } else if ((float)array[i][3]/(float)array[i][4] > 0.6) {
                        consensus = consensus + "G";
                        gap_count = 0;
                    } else {
                        consensus = consensus + "N";
                        N_count = N_count + 1;
                        gap_count = gap_count + 1;
                    }
                    if (gap_count >= 4 && consensus_len >= 20) {
                        return null;
                    }
                }
                if ( (float)N_count/(float)consensus.length() > 0.4 && consensus_len >= 20) {
                    return null;
                }
                //\\  Select best edge
                Collections.sort(black_edges, new BlackEdgeComparator2());
                for(int i=0; i < black_edges.size(); i++) {
                    //\\
                    String [] vals = black_edges.get(i).split("\\|");
                    if (vals[0].equals(dir + "f") || vals[0].equals(dir +"r")){
                        best_edge = black_edges.get(i);
                        break;
                        /*String str = Node.dna2str(vals[2]);
                        if (vals[0].charAt(1) == 'r'){
                            str = Node.rc(str);
                        }
                        String str_cut = "";
                        str_cut = str.substring(Integer.parseInt(vals[3]));
                        int count_match = 0;
                        int compare_len = 0;
                        for(int j=0; j < str_cut.length() && j < consensus_len; j++ ) {
                            if (str_cut.charAt(j) == consensus.charAt(j)) {
                                count_match = count_match + 1;
                            }
                            compare_len = j;
                        }
                        if ((float)count_match/(float)compare_len > 0.5) {
                            return black_edges.get(i);
                        }*/
                    }
                    //\\
                    
                }
                
            } else { // edge_list.size <= 1
                String [] vals = black_edges.get(0).split("\\|");
                if (vals[0].equals(dir + "f") || vals[0].equals(dir +"r")){
                    best_edge = black_edges.get(0);
                }
                
            }
            //\\\\\\\\\\
        }
        return best_edge;
    }

    // Sort edge_list with length
    public static String Consensus(List<String> edge_list , float majority, float threshold) {
        String consensus = null;
        class ConsensusComparator implements Comparator {
            public int compare(Object element1, Object element2) {
                String obj1 = (String) element1;
                String obj2 = (String) element2;
                //con + "|" + node_id + "|" + str_raw + "|" + oval_size + "|" + cov + "|" + end;
                String[] val1 = obj1.split("!");
                String[] val2 = obj2.split("!");
                if ((int) ( val1[0].length()-val2[0].length())  >= 0) {
                    return -1;
                } else {
                    return 1;
                }
            }
        }
        //\\\
        Collections.sort(edge_list, new ConsensusComparator());
        if (edge_list.size() >= 2) {
            //int consensus_len = edge_list.get(1).substring(0, edge_list.get(1).indexOf("!")).length();
            String [] compare_edge0 = edge_list.get(0).split("!");
            String [] compare_edge1 = edge_list.get(1).split("!");
            int consensus_len;
            if (edge_list.size() == 2 || Float.parseFloat(compare_edge0[1]) + Float.parseFloat(compare_edge1[1]) > 2  ) {
                consensus_len = edge_list.get(1).substring(0, edge_list.get(1).indexOf("!")).length();
            } else {
                consensus_len = edge_list.get(2).substring(0, edge_list.get(2).indexOf("!")).length();
            } 
            //\\ 0:A 1:T 2:C 3:G 4:Sum
            int[][] array = new int[consensus_len][5];
            for(int i=0; i < consensus_len; i++) {
                for(int j=0; j < 5; j++) {
                    array[i][j] = 0;
                }
            }
            for(int i=0; i < edge_list.size(); i++) {
                String [] compare_edge = edge_list.get(i).split("!");
                for(int j=0; j < compare_edge[0].length() && j < consensus_len; j++ ) {
                    array[j][4] = array[j][4] + (int)Float.parseFloat(compare_edge[1]);
                    if (compare_edge[0].charAt(j) == 'A'){
                        array[j][0] = array[j][0] + (int)Float.parseFloat(compare_edge[1]);
                    } else if (compare_edge[0].charAt(j) == 'T'){
                        array[j][1] = array[j][1] + (int)Float.parseFloat(compare_edge[1]);
                    } else if (compare_edge[0].charAt(j) == 'C'){
                        array[j][2] = array[j][2] + (int)Float.parseFloat(compare_edge[1]);
                    } else if (compare_edge[0].charAt(j) == 'G'){
                        array[j][3] = array[j][3] + (int)Float.parseFloat(compare_edge[1]);
                    }
                }
            }
            // construct consensus
            int N_count = 0;
            int gap_count = 0;
            consensus = "";
            for(int i=0; i < array.length; i++){
                if ((float)array[i][0]/(float)array[i][4] > majority) {
                    consensus = consensus + "A";
                    gap_count = 0;
                } else if ((float)array[i][1]/(float)array[i][4] > majority) {
                    consensus = consensus + "T";
                    gap_count = 0;
                } else if ((float)array[i][2]/(float)array[i][4] > majority) {
                    consensus = consensus + "C";
                    gap_count = 0;
                } else if ((float)array[i][3]/(float)array[i][4] > majority) {
                    consensus = consensus + "G";
                    gap_count = 0;
                } else {
                    consensus = consensus + "N";
                    N_count = N_count + 1;
                    gap_count = gap_count + 1;
                }
                /*if (gap_count >= 4) {
                    return null;
                }*/
            }
            if ( (float)N_count/(float)consensus_len > threshold /*&& consensus_len >= len*/) {
                return null;
            }
        } else if (edge_list.size() == 1) {
            return edge_list.get(0).substring(0, edge_list.get(0).indexOf("!"));
        }
        //\\\
        return consensus;
    }

    public static String Consensus2(List<String> edge_list , float threshold){
        String consensus = null;
        class ConsensusComparator implements Comparator {
            public int compare(Object element1, Object element2) {
                String obj1 = (String) element1;
                String obj2 = (String) element2;
                //con + "|" + node_id + "|" + str_raw + "|" + oval_size + "|" + cov + "|" + end;
                String[] val1 = obj1.split("!");
                String[] val2 = obj2.split("!");
                if ((int) ( val1[0].length()-val2[0].length())  >= 0) {
                    return -1;
                } else {
                    return 1;
                }
            }
        }
        //\\\
        Collections.sort(edge_list, new ConsensusComparator());
        if (edge_list.size() >= 2) {
            int consensus_len = edge_list.get(1).substring(0, edge_list.get(1).indexOf("!")).length();
            //\\ 0:A 1:T 2:C 3:G 4:Sum
            int[][] array = new int[consensus_len][5];
            for(int i=0; i < consensus_len; i++) {
                for(int j=0; j < 5; j++) {
                    array[i][j] = 0;
                }
            }
            for(int i=0; i < edge_list.size(); i++) {
                String [] compare_edge = edge_list.get(i).split("!");
                for(int j=0; j < compare_edge[0].length() && j < consensus_len; j++ ) {
                    array[j][4] = array[j][4] + (int)Float.parseFloat(compare_edge[1]);
                    if (compare_edge[0].charAt(j) == 'A'){
                        array[j][0] = array[j][0] + (int)Float.parseFloat(compare_edge[1]);
                    } else if (compare_edge[0].charAt(j) == 'T'){
                        array[j][1] = array[j][1] + (int)Float.parseFloat(compare_edge[1]);
                    } else if (compare_edge[0].charAt(j) == 'C'){
                        array[j][2] = array[j][2] + (int)Float.parseFloat(compare_edge[1]);
                    } else if (compare_edge[0].charAt(j) == 'G'){
                        array[j][3] = array[j][3] + (int)Float.parseFloat(compare_edge[1]);
                    }
                }
            }
            // construct consensus
            int N_count = 0;
            consensus = "";
            for(int i=0; i < array.length; i++){
                if (array[i][0] > array[i][1] && array[i][0] > array[i][2] && array[i][0] > array[i][3]) {
                    consensus = consensus + "A";
                } else if (array[i][1] > array[i][0] && array[i][1] > array[i][2] && array[i][1] > array[i][3]) {
                    consensus = consensus + "T";
                } else if (array[i][2] > array[i][0] && array[i][2] > array[i][1] && array[i][2] > array[i][3]) {
                    consensus = consensus + "C";
                } else if (array[i][3] > array[i][0] && array[i][3] > array[i][1] && array[i][3] > array[i][2]) {
                    consensus = consensus + "G";
                } else {
                    consensus = consensus + "N";
                    N_count = N_count + 1;
                }
             
            }
            if ( (float)N_count/(float)consensus_len > threshold ) {
                return null;
            }
        } else if (edge_list.size() == 1) {
            return edge_list.get(0).substring(0, edge_list.get(0).indexOf("!"));
        }
        //\\\
        return consensus;
    }
    
    public void addBlackEdge(String con, String node_id, String str_raw, int oval_size, float cov, String end)
	{
		String msg = con + "|" + node_id + "|" + str_raw + "|" + oval_size + "|" + cov + "|" + end;

		List<String> l = getOrAddField(BLACKEDGE);
		l.add(msg);
	}

    public void setBlackEdges(List<String> edges)
	{
		if (edges == null)
		{
			fields.remove(BLACKEDGE);
		}
		else
		{
            if (edges.size() == 0){
                fields.remove(BLACKEDGE);
            } else {
                fields.put(BLACKEDGE, edges);
            }
		}
	}

    public List<String> getBlackEdges()
	{
		if (fields.containsKey(BLACKEDGE))
		{
			return fields.get(BLACKEDGE);
		}

		return null;
	}

    public List<String> getBlackEdges(String adj)
	{
        List<String> tmp = new ArrayList<String>();  
		if (fields.containsKey(BLACKEDGE))
		{
            List<String> l;   
            l = getOrAddField(BLACKEDGE);
            for (int i=0; i < l.size(); i++){
                String [] msg = l.get(0).split("\\|");
                if (msg[0].substring(0,1).equals(adj)) {
                    tmp.add(l.get(i));
                }
            }	
		}
        return tmp;
		//return null;
	}
    
    public void clearBlackEdge() {
        fields.remove(BLACKEDGE);
    }

     public void clearBlackEdge(String adj){
        List<String> l;
        List<String> tmp = new ArrayList<String>();
        l = getOrAddField(BLACKEDGE);
        for (int i=0; i < l.size(); i++){
            String [] msg = l.get(0).split("\\|");
            if (!msg[0].substring(0,1).equals(adj)) {
                tmp.add(l.get(i));
            }
        }
		fields.remove(BLACKEDGE);
        if (tmp.size() > 0) {
            fields.put(BLACKEDGE, tmp);
        }
    }

    public boolean isBlack(String dir) throws IOException
    {
        Vector<String> black_id = new Vector<String>();
        List<String> black_edges = getBlackEdges();
        if (black_edges != null) {
            for(int i=0; i < black_edges.size(); i++) {
                String [] vals = black_edges.get(i).split("\\|");
                if (vals[0].equals(dir+"r") || vals[0].equals(dir+"f")) {
                    black_id.add(vals[1]);
                }
            }
        }
        for(String adj : Node.dirs) {
            List<String> edges = getEdges(dir+adj);
            if (edges != null) {
                for(int i=0; i < edges.size(); i++){
                    String[] vals = edges.get(i).split("!");
                    if (!black_id.contains(vals[0])) {
                        return false;
                    }
                }
            }
        }
        return true;
    }

    // possible isBlack
    public boolean isGray(String dir) throws IOException{
        Vector<String> black_id = new Vector<String>();
        Vector<String> gray_id = new Vector<String>();
        List<String> black_edges = getBlackEdges();
        List<String> gray_edges = getGrayEdges();
        if (black_edges != null){
            for(int i=0; i < black_edges.size(); i++) {
                String [] vals = black_edges.get(i).split("\\|");
                if (vals[0].equals(Node.flip_dir(dir)+"r") || vals[0].equals(Node.flip_dir(dir)+"f")) {
                    black_id.add(vals[1]);
                }
            }
        }
        if (gray_edges != null) {
            for(int i=0; i < gray_edges.size(); i++) {
                String [] vals = gray_edges.get(i).split("\\|");
                if (vals[0].equals(Node.flip_dir(dir)+"r") || vals[0].equals(Node.flip_dir(dir)+"f")) {
                    gray_id.add(vals[1]);
                }
            }
        }
        for(String adj : Node.dirs) {
            List<String> edges = getEdges(Node.flip_dir(dir)+adj);
            if (edges != null ){
                for(int i=0; i < edges.size(); i++){
                    String[] vals = edges.get(i).split("!");
                    if (gray_id.contains(vals[0])) {
                        return true;
                    }
                    if (black_id.contains(vals[0])) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    public int blkdegree(String dir)
	{
		int retval = 0;

		List<String> black_edges = getBlackEdges();
        if (black_edges != null) {
            for(int i=0; i < black_edges.size(); i++) {
                String [] vals = black_edges.get(i).split("\\|");
                if (vals[0].equals(dir+"r") || vals[0].equals(dir+"f")) {
                    retval = retval + 1;
                }
            }
        }

		return retval;
	}

    public void addPairEnd( String v)
	{
		/*List<String> l = getOrAddField(MATE);
        if (!l.contains(v)){
            l.add(v);
        }*/
	}
    
    public void addAllPairEnd( List<String> reads)
	{
		/*List<String> l = getOrAddField(MATE);
        if (l == null){
            fields.put(MATE, filterAllPair(reads));
        } else {
            l.addAll(reads);
            fields.put(MATE, filterAllPair(l));
        }*/
	}
    
    public List<String> filterAllPair(List<String> reads){
        List<String> verify_reads = new ArrayList();
        for(int i=0; i < reads.size(); i++) {
            if (!verify_reads.contains(reads.get(i))){
                verify_reads.add(reads.get(i));
            }
        }
        return verify_reads;
    }

    public void setPairEnds(List<String> reads)
	{
		/*if (reads == null)
		{
			fields.remove(MATE);
		}
		else
		{
            if (reads.size() == 0){
                fields.remove(MATE);
            } else {
                fields.put(MATE, filterAllPair(reads));
            }
		}*/
	}

    public List<String> getPairEnds()
	{
		if (fields.containsKey(MATE))
		{
			return fields.get(MATE);
		} else {
          List<String> self = new ArrayList<String>();
          self.add(this.nodeid);
          return self;  
        }

		//return null;
	}
    
    public void addReadAttribute( String v)
	{
		List<String> l = getOrAddField(ATTRIBUTE);
        if (!l.contains(v)){
            l.add(v);
        }
	}
    
    public List<String> getReadAttribute()
	{
		if (fields.containsKey(ATTRIBUTE))
		{
			return fields.get(ATTRIBUTE);
		}

		return null;
	}
    
    public void clearReadAttribute() {
        fields.remove(ATTRIBUTE);
    }
    
    public void addContainedReads( String v)
	{
		List<String> l = getOrAddField(CONTAINED);
        if (!l.contains(v)){
            l.add(v);
        }
	}

    public void setContainedReads(List<String> reads)
	{
		if (reads == null)
		{
			fields.remove(CONTAINED);
		}
		else
		{
            if (reads.size() == 0){
                fields.remove(CONTAINED);
            } else {
                fields.put(CONTAINED, reads);
            }
		}
	}

    public List<String> getContainedReads()
	{
		if (fields.containsKey(CONTAINED))
		{
			return fields.get(CONTAINED);
		}

		return null;
	}
    
    public List<String> getContainedReads_update() throws IOException
	{
        List<String> tmp = new ArrayList();
		for(String key: Node.edgetypes) {
            List<String> edges = getEdges(key);
            if (edges != null)
            {
                for (String p : edges)
                {
                    String[] vals = p.split("!");
                    String edge_id = vals[0];
                    int oval_size = Integer.parseInt(vals[1]);
                    if (oval_size < 0){
                        tmp.add(edge_id);
                    }
                }
            }
            
        }
        if (tmp.size() > 0) {
            return tmp;
        } else {
            return null;
        }
	}

    public void clearContainedReads() {
        fields.remove(CONTAINED);
    }

    public String getNodeId() { return nodeid; }

	public void setNodeId(String nid) { nodeid = nid; }

    public String toNodeMsg()
	{
		return toNodeMsg(false);
	}

    public String toNodeMsg(boolean tagfirst)
	{
		StringBuilder sb = new StringBuilder();

		DecimalFormat df = new DecimalFormat("0.00");

		if (tagfirst)
		{
			sb.append(nodeid);
			sb.append("\t");
		}

		sb.append(NODEMSG);

		sb.append("\t*"); sb.append(STR);
		sb.append("\t"); sb.append(str_raw());

		sb.append("\t*"); sb.append(COVERAGE);
		sb.append("\t"); sb.append(df.format(cov()));

		for(String t : edgetypes)
		{
			if (fields.containsKey(t))
			{
				sb.append("\t*"); sb.append(t);

				for(String i : fields.get(t))
				{
					sb.append("\t"); sb.append(i);
				}
			}
		}

		char [] dirs = {'f', 'r'};

		for(char d : dirs)
		{
			String t = CANCOMPRESS + d;
			if (fields.containsKey(t))
			{
				sb.append("\t*"); sb.append(t);
				sb.append("\t");  sb.append(fields.get(t).get(0));
			}
		}

        if (fields.containsKey(UNIQUE))
        {
            sb.append("\t*"); sb.append(UNIQUE);
            sb.append("\t");  sb.append(fields.get(UNIQUE).get(0));
        }

		if (fields.containsKey(MERGE))
		{
			sb.append("\t*"); sb.append(MERGE);
			sb.append("\t");  sb.append(fields.get(MERGE).get(0));
		}
        
        if (fields.containsKey(REPLACEMENT))
		{
			sb.append("\t*"); sb.append(REPLACEMENT);
			sb.append("\t");  sb.append(fields.get(REPLACEMENT).get(0));
		}

		if (!tagfirst && fields.containsKey(MERTAG))
		{
			sb.append("\t*"); sb.append(MERTAG);
			sb.append("\t");  sb.append(fields.get(MERTAG).get(0));
		}

	/*	if (fields.containsKey(THREAD))
		{
			sb.append("\t*"); sb.append(THREAD);
			for(String t : fields.get(THREAD))
			{
				sb.append("\t"); sb.append(t);
			}
		}*/

	/*	if (fields.containsKey(THREADPATH))
		{
			sb.append("\t*"); sb.append(THREADPATH);
			for(String t : fields.get(THREADPATH))
			{
				sb.append("\t"); sb.append(t);
			}
		}*/

	/*	if (fields.containsKey(THREADIBLEMSG))
		{
			sb.append("\t*"); sb.append(THREADIBLEMSG);
			for(String t : fields.get(THREADIBLEMSG))
			{
				sb.append("\t"); sb.append(t);
			}
		}*/

		if (fields.containsKey(POPBUBBLE))
		{
			sb.append("\t*"); sb.append(POPBUBBLE);
			for(String t : fields.get(POPBUBBLE))
			{
				sb.append("\t"); sb.append(t);
			}
		}

        if (fields.containsKey(CONTAINED))
		{
			sb.append("\t*"); sb.append(CONTAINED);
			for(String t : fields.get(CONTAINED))
			{
				sb.append("\t"); sb.append(t);
			}
		}
        
        if (fields.containsKey(ATTRIBUTE))
		{
			sb.append("\t*"); sb.append(ATTRIBUTE);
			for(String t : fields.get(ATTRIBUTE))
			{
				sb.append("\t"); sb.append(t);
			}
		}

        if (fields.containsKey(MATE))
		{
			sb.append("\t*"); sb.append(MATE);
			for(String t : fields.get(MATE))
			{
				sb.append("\t"); sb.append(t);
			}
		}
        
        if (fields.containsKey(REMOVEDGE))
		{
			sb.append("\t*"); sb.append(REMOVEDGE);
			for(String t : fields.get(REMOVEDGE))
			{
				sb.append("\t"); sb.append(t);
			}
		}
        
        if (fields.containsKey(ADJUSTEDGE))
		{
			sb.append("\t*"); sb.append(ADJUSTEDGE);
			for(String t : fields.get(ADJUSTEDGE))
			{
				sb.append("\t"); sb.append(t);
			}
		}

        if (fields.containsKey(MERGETIPS))
		{
			sb.append("\t*"); sb.append(MERGETIPS);
			for(String t : fields.get(MERGETIPS))
			{
				sb.append("\t"); sb.append(t);
			}
		}


        if (fields.containsKey(FCOLOR))
		{
			sb.append("\t*"); sb.append(FCOLOR);
			for(String t : fields.get(FCOLOR))
			{
				sb.append("\t"); sb.append(t);
			}
		}

        if (fields.containsKey(RCOLOR))
		{
			sb.append("\t*"); sb.append(RCOLOR);
			for(String t : fields.get(RCOLOR))
			{
				sb.append("\t"); sb.append(t);
			}
		}

        if (fields.containsKey(GRAYEDGE))
		{
			sb.append("\t*"); sb.append(GRAYEDGE);
			for(String t : fields.get(GRAYEDGE))
			{
				sb.append("\t"); sb.append(t);
			}
		}

        if (fields.containsKey(BLACKEDGE))
		{
			sb.append("\t*"); sb.append(BLACKEDGE);
			for(String t : fields.get(BLACKEDGE))
			{
				sb.append("\t"); sb.append(t);
			}
		}


	/*	if (fields.containsKey(R5))
		{
			sb.append("\t*"); sb.append(R5);
			for(String r : fields.get(R5))
			{
				sb.append("\t"); sb.append(r);
			}
		}*/

	/*	if (fields.containsKey(BUNDLE))
		{
			sb.append("\t*"); sb.append(BUNDLE);
			for(String r : fields.get(BUNDLE))
			{
				sb.append("\t"); sb.append(r);
			}
		}*/

	/*	if (fields.containsKey(MATETHREAD))
		{
			sb.append("\t*"); sb.append(MATETHREAD);
			for(String r : fields.get(MATETHREAD))
			{
				sb.append("\t"); sb.append(r);
			}
		}*/

		return sb.toString();
	}

    public void fromNodeMsg(String nodestr) throws IOException
	{
		fields.clear();

		String [] items = nodestr.split("\t");

		nodeid = items[0];
		parseNodeMsg(items, 1);
	}

	public void parseNodeMsg(String[] items, int offset) throws IOException
	{
		if (!items[offset].equals(NODEMSG))
		{
			throw new IOException("Unknown code: " + items[offset]);
		}

		List<String> l = null;

		offset++;

		while (offset < items.length)
		{
			if (items[offset].charAt(0) == '*')
			{
				String type = items[offset].substring(1);
				l = fields.get(type);

				if (l == null)
				{
					l = new ArrayList<String>();
					fields.put(type, l);
				}
			}
			else if (l != null)
			{
				l.add(items[offset]);
			}

			offset++;
		}
	}

    public void fromNodeMsg(String nodestr, Set<String> desired)
	{
		fields.clear();

		String [] items = nodestr.split("\t");
		List<String> l = null;

		// items[0] == nodeid
		// items[1] == NODEMSG

		for (int i = 2; i < items.length; i++)
		{
			if (items[i].charAt(0) == '*')
			{
				l = null;

				String type = items[i].substring(1);

				if (desired.contains(type))
				{
					l = fields.get(type);

					if (l == null)
					{
						l = new ArrayList<String>();
						fields.put(type, l);
					}
				}
			}
			else if (l != null)
			{
				l.add(items[i]);
			}
		}
	}

    public static String flip_dir(String dir) throws IOException
	{
		if (dir.equals("f")) { return "r"; }
		if (dir.equals("r")) { return "f"; }

		throw new IOException("Unknown dir type: " + dir);
	}

    public static String flip_link(String link) throws IOException
	{
		if (link.equals("ff")) { return "rr"; }
		if (link.equals("fr")) { return "fr"; }
		if (link.equals("rf")) { return "rf"; }
		if (link.equals("rr")) { return "ff"; }
		throw new IOException("Unknown link type: " + link);
	}

    public static String rc(String seq) //reverse complement
	{
		StringBuilder sb = new StringBuilder();

		for (int i = seq.length() - 1; i >= 0; i--)
		{
			if      (seq.charAt(i) == 'A') { sb.append('T'); }
			else if (seq.charAt(i) == 'T') { sb.append('A'); }
			else if (seq.charAt(i) == 'C') { sb.append('G'); }
			else if (seq.charAt(i) == 'G') { sb.append('C'); }
		}

		return sb.toString();
	}

    public static void main(String[] args) throws Exception
	{
        Node node = new Node();
        node.setColor(Color.W, "r");
        node.setColor(Color.B, "f");
        node.setstr_raw("ACGT");
        node.setCoverage(1);
        node.addBlackEdge("ff", "1", "ACGT", 4, 1, "");
        node.addBlackEdge("rf", "5", "CGT", 2, 1, "");
        node.addGrayEdge("rr", "3");
        node.isBlack("f");
        System.out.println(fastdistance("XYX", "XYY"));
        System.out.println(node.getColor("r"));
        System.out.println(node.toNodeMsg());
    }
}
