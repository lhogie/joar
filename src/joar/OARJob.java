package joar;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.Serializable;
import java.util.Properties;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import toools.extern.Proces;
import toools.io.Cout;
import toools.thread.MultiThreadProcessing;

public class OARJob implements Serializable {
	public static final long serialVersionUID = - 8091490396271431387L;

	private final String frontal;
	private final int id;
	private final String name;

	public OARJob(String frontal, int id, String name) {
		this.id = id;
		this.frontal = frontal;
		this.name = name;
	}

	public static OARJob submit(String frontal, String oarsubParms, String cmd,
			String name) {
		// ClassPath.retrieveSystemClassPath().rsyncTo(frontal);

		byte[] stdout = Proces.exec("ssh", frontal, "oarsub", oarsubParms, "--name",
				"JOAR::" + name, "'" + cmd + "'");
		Cout.debugSuperVisible(cmd);
		Pattern p = Pattern.compile("OAR_JOB_ID=([0-9]+)");

		for (String line : new String(stdout).split("\n")) {
			System.out.println(line);
			Matcher m = p.matcher(line);

			// if we find a match, get the group
			if (m.find()) {
				// we're only looking for one group, so get it
				String theGroup = m.group(1);
				int id = Integer.valueOf(theGroup);
				return new OARJob(frontal, id, name);
			}
		}

		throw new IllegalStateException();
	}

	public void delete() {
		Proces.exec("ssh", frontal, "oardel", "" + id);
	}

	public String getSdtOutFileName() {
		return "OAR.JOAR::" + name + "." + id + ".stdout";
	}

	public String getSdtErrFileName() {
		return "OAR.JOAR::" + name + "." + id + ".stderr";
	}

	public String getStdOut() {
		return new String(Proces.exec("ssh", frontal, "cat", getSdtOutFileName()));
	}

	public String getStdErr() {
		return new String(Proces.exec("ssh", frontal, "cat", getSdtErrFileName()));
	}

	public int getID() {
		return id;
	}

	public void connect() {
		waitForStdIOFiles();

		new MultiThreadProcessing(2, "streaming OAR output", null) {
			@Override
			protected void runInParallel(ThreadSpecifics t) {
				if (t.rank == 0) {
					streamFile(getSdtOutFileName(), System.out);
				}
				else {
					streamFile(getSdtErrFileName(), System.err);
				}
			}
		}.execute();
	}

	public void streamFile(String filename, PrintStream out) {
		try {
			Process p = Runtime.getRuntime()
					.exec("ssh " + frontal + " tail -n +1 -f " + filename);

			BufferedReader is = new BufferedReader(
					new InputStreamReader(p.getInputStream()));

			while (true) {
				String s = is.readLine();

				if (s == null)
					break;

				out.println(s);
			}
		}
		catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void waitForStdIOFiles() {
		System.out.println("waiting for " + getSdtOutFileName());
		Proces.exec("ssh", frontal,
				"while ! test -f '" + getSdtOutFileName() + "'; do sleep 1; done");
	}

	public Properties retrieveInfo() {
		try {
			Properties p = new Properties();
			p.load(new ByteArrayInputStream(
					Proces.exec("ssh", frontal, "oarstat -f -j " + id)));
			return p;
		}
		catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}

	public static int retrieveJobID(String frontal, String name) {
		for (String line : new String(Proces.exec("ssh", frontal, "oarstat"))
				.split("\n")) {
			if (line.contains("JOAR::" + name)) {
				return new Scanner(line).nextInt();
			}
		}

		return - 1;
	}

	public static OARJob getOrCreate(String frontal, String resourceRequested, String cmd,
			String name) {
		int id = OARJob.retrieveJobID(frontal, name);

		if (id == - 1) {
			return OARJob.submit(frontal, resourceRequested, cmd, name);
		}
		else {
			return new OARJob(frontal, id, name);
		}
	}
}
