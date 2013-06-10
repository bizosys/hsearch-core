package com.bizosys.hsearch.admin;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.bizosys.hsearch.treetable.compiler.HSearchCompiler;
import com.sun.tools.javac.Main;

public class HSearchShell extends Configured implements Tool {

	
	public String SRC_TEMP = "/tmp/src";
	public String BUILD_TEMP = "/tmp/build";
	public String JAR_TEMP = "/tmp/jar";
	
	
	public void createTable(String schemaFile){

		File tmpDir = new File("/tmp");
		if ( ! tmpDir.exists() ) {
			tmpDir.mkdir();
		} else {
			if ( ! tmpDir.isDirectory() ) {
				System.err.println("found /tmp file , expecting /tmp directory");
				return;
			}
		}
		
		double x = Math.random();
		long uuid = new Double( Long.MAX_VALUE * x).longValue();
		//uuid = 804677596611081216L;
		SRC_TEMP = SRC_TEMP.replace("/tmp", "/tmp/" + uuid);
		BUILD_TEMP = BUILD_TEMP.replace("/tmp", "/tmp/" + uuid);
		JAR_TEMP = JAR_TEMP.replace("/tmp", "/tmp/" + uuid);
		
		File workDirectory = new File("/tmp/" + uuid);
		workDirectory.mkdir();
		
		try {
			File schemaFileObject = new File(schemaFile);
			
			if(!schemaFileObject.exists()){
				System.err.println("Error: File " + schemaFileObject.getAbsolutePath()+ " not found.!");
				return;
			}
			
			//create directories
			File srcFile = new File(SRC_TEMP);
			if ( ! srcFile.exists() ) {
				if(! srcFile.mkdirs()){
					System.err.println("Not able to create directory : " + srcFile.getAbsolutePath());
					return;
				}
			}

			File buildFile = new File(BUILD_TEMP);
			if ( ! buildFile.exists() ) {
				if(! buildFile.mkdirs()){
					System.err.println("Not able to create directory : " + buildFile.getAbsolutePath());
					return;
				}
			}
			
			File jarFile = new File(JAR_TEMP);
			if ( ! jarFile.exists() ) {
				if(! jarFile.mkdirs()){
					System.err.println("Not able to create directory : " + jarFile.getAbsolutePath());
					return;
				}
			}

			HSearchCompiler.main(new String[]{schemaFile,SRC_TEMP});
			
			
			File javaSourceDirectory = new File(SRC_TEMP);
			List<String> sources = new ArrayList<String>();
			listAllFiles(javaSourceDirectory, sources, ".java");
			
			String[] compileArgs = new String[sources.size() + 4];
			int index = 0;
			compileArgs[index++] = "-cp";
			compileArgs[index++] = System.getProperty("java.class.path") + ";" + BUILD_TEMP;
			compileArgs[index++] = "-d";
			compileArgs[index++] = BUILD_TEMP;
			for (String source : sources) {
				compileArgs[index++] = source;
			}
			
			//compile
			Main.compile(compileArgs);
			
			
			//make jar
			Manifest manifest = new Manifest();
			manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
			String jarFileName = JAR_TEMP + "/hsearch-" + 
					schemaFileObject.getName().substring(0, 
							schemaFileObject.getName().indexOf('.')) + ".jar";
			JarOutputStream target = new JarOutputStream(new FileOutputStream(jarFileName), manifest);
			
			System.out.println("\n\nBuilding Jar : " + jarFileName );
			createJar(new File(BUILD_TEMP), target);
			target.close();
			System.out.println("\tCreated [" + jarFileName + "]");
			
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void listAllFiles(File javaSourceDirectory, List<String> sources, String extension) {
		if ( javaSourceDirectory.isDirectory() ) {
			File[] fileOrDirectoryL = javaSourceDirectory.listFiles();
			for (File fileOrDirectory : fileOrDirectoryL) {
				if ( fileOrDirectory.isDirectory())  {
					listAllFiles(fileOrDirectory, sources,extension);
					continue;
				}
				if (fileOrDirectory.getName().endsWith(extension) )
					sources.add(fileOrDirectory.getAbsolutePath());
			}
		} else {
			if (javaSourceDirectory.getName().endsWith(extension) )
				sources.add(javaSourceDirectory.getAbsolutePath());
		}
	}
	
	private void createJar(File source, JarOutputStream target) throws IOException
	{
		//Find all files @ this directory
		List<File> allFiles = new ArrayList<File>();
		if ( source.isDirectory()) {
			for (File file : source.listFiles()) {
				allFiles.add(file);
			}
		} else {
			allFiles.add(source);
		}
		
		//Recurse if it's a directory.
		for (File nestedFile: allFiles) {
			
		    if (nestedFile.isDirectory()) {
		    	createJar(nestedFile, target);
		    	continue;
		    }
		
			String name = nestedFile.getPath().replace("\\", "/");
			if (name.isEmpty()) continue;
			name = name.replace(BUILD_TEMP + "/" , "");
			if ( name.equals("")) continue; 
			
			System.out.print(".");
			JarEntry entry = new JarEntry(name);
			entry.setTime(nestedFile.lastModified());
			target.putNextEntry(entry);
			
			BufferedInputStream in = new BufferedInputStream(new FileInputStream(nestedFile));
		    byte[] buffer = new byte[1024];
		    while (true)
		    {
		      int count = in.read(buffer);
		      if (count == -1)
		        break;
		      target.write(buffer, 0, count);
		    }
		    target.closeEntry();	        
	    }
	}


	  protected void init() throws IOException {
	  }
	
	  /**
	   * run
	   */
	  public int run(String argv[]) throws Exception {

	    if (argv.length < 1) {
	      printUsage(""); 
	      return -1;
	    }

	    int exitCode = -1;
	    int i = 0;
	    String cmd = argv[i++];
	    //
	    // verify that we have enough command line parameters
	    //
	    if ("-create".equals(cmd) ) {
	    	if (argv.length != 2) {
	    		printUsage(cmd);
	    		return exitCode;
	    	}
	    } else if ("-search".equals(cmd) ) {
	    	if (argv.length < 3) {
	    		printUsage(cmd);
	    		return exitCode;
	    	}
	    } else if ("-count".equals(cmd) ) {
	    	if (argv.length < 3) {
	    		printUsage(cmd);
	    		return exitCode;
	    	}
	    }

	    // initialize FsShell
	    try {
	      init();
	    } catch (RPC.VersionMismatch v) { 
	      System.err.println("Version Mismatch between client and server" + "... command aborted.");
	      return exitCode;
	    } catch (IOException e) {
	      System.err.println("Bad connection to HBase. command aborted.");
	      return exitCode;
	    }

	    try {
	      if ("-create".equals(cmd)) {
	        this.createTable(argv[1]);
	      } else if ("-search".equals(cmd)) {
	      } else if ("-count".equals(cmd)) {
	      } else {
	        System.err.println(cmd.substring(1) + ": Unknown command");
	        printUsage("");
	      }
	    } catch (IllegalArgumentException arge) {
	      exitCode = -1;
	      System.err.println(cmd.substring(1) + ": " + arge.getLocalizedMessage());
	      printUsage(cmd);
	    } catch (Exception re) {
	      exitCode = -1;
	      System.err.println(cmd.substring(1) + ": " + re.getLocalizedMessage());  
	    } finally {
	    }
	    return exitCode;
	  }

	  public void close() throws IOException {
	  }
	
	  /**
	   * Displays format of commands.
	   * 
	   */
	  private static void printUsage(String cmd) {
	    String prefix = "Usage: java " + HSearchShell.class.getSimpleName();
	    if ("-create".equals(cmd)) {
	      System.err.println("Usage: java HSearchShell " + 
	    		  " [-create <file system scema file URI>]");
	    } else if ("-search".equals(cmd)) {
	      System.err.println("Usage: java HSearchShell" + 
	                         " [-search <table_name> <query>]");
	    } else {
	      System.err.println("Usage: java HSearchShell");
	      System.err.println("           [-create <schema path>]");
	      System.err.println("           [-search <table_name> <query>]");
	      System.err.println("           [-help [cmd]]");
	      System.err.println();
	    }
	  }
	  
	  /**
	   * main() has some simple utility methods
	   */
	  public static void main(String argv[]) throws Exception {
	    HSearchShell shell = new HSearchShell();
	    int res;
	    try {
	      res = ToolRunner.run(shell, argv);
	    } finally {
	      shell.close();
	    }
	    System.exit(res);
	  }

	
}
