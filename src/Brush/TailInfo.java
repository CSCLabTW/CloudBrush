/*
    TallInfo.java
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
import java.util.Map;
import java.util.Set;

public class TailInfo
{
	public String id;
	public String dir;
	public int    dist;
    public int    oval_size;

	public TailInfo(TailInfo o)
	{
		id   = o.id;
		dir  = o.dir;
		dist = o.dist;
        oval_size = o.oval_size;
	}

	public TailInfo()
	{
		id = null;
		dir = null;
		dist = 0;
        oval_size = 0;
	}

	public String toString()
	{
		if (this == null)
		{
			return "null";
		}

		return id + " " + dir + " " + dist;
	}

	public static TailInfo find_tail(Map<String, Node> nodes, Node startnode, String startdir) throws IOException
	{
		//System.err.println("find_tail: " + startnode.getNodeId() + " " + startdir);
		Set<String> seen = new HashSet<String>();
		seen.add(startnode.getNodeId());

		Node curnode = startnode;
		String curdir = startdir;
		String curid = startnode.getNodeId();
		int dist = 0;
        int oval_size = 0;

		boolean canCompress = false;

		do
		{
			canCompress = false;

			TailInfo next = curnode.gettail(curdir);

			//System.err.println(curnode.getNodeId() + " " + curdir + ": " + next);

			if ((next != null) &&
				(nodes.containsKey(next.id)) &&
				(!seen.contains(next.id)))
			{
				seen.add(next.id);
				curnode = nodes.get(next.id);

				//\\// ��靘Ⅱ隤���node (銋��urnode curid隞�”�ode) �臬�畚ompress
                TailInfo nexttail = curnode.gettail(Node.flip_dir(next.dir));

				if ( (curnode.getBlackEdges() == null) && (nexttail != null) && (nexttail.id.equals(curid)))
				{
					dist++;
					canCompress = true;

					curid = next.id;
					curdir = next.dir;
                    oval_size = next.oval_size;
				}
			}
		}
		while (canCompress);

		TailInfo retval = new TailInfo();

		retval.id = curid;
		retval.dir = Node.flip_dir(curdir);
		retval.dist = dist;
        retval.oval_size = oval_size;

		return retval;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
