package joar;

import java.io.IOException;

public class Demo
{
	public static void main(String[] args) throws IOException
	{
		String frontal = "nef-frontal.inria.fr";
		String cmd = "'java -classpath $(cat $HOME/.classpath) -Xmx200G jmaxgraph.cmd.adj2jmg --nbThread=8 /home/lhogie/biggrph/datasets/twitter.adj  $HOME/twitter'";
		String resourceRequested = "-p 'mem > 180000' -l '/nodes=1,walltime=15:0:0'";
		String name = "foobar";
		OARJob j = OARJob.getOrCreate(frontal, resourceRequested, cmd, name);

//		j.askInfo().save(System.out, "");

		j.connect();
	}

}
