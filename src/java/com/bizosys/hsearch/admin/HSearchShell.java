package com.bizosys.hsearch.admin;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

import com.bizosys.hsearch.treetable.compiler.HSearchCompiler;
import com.sun.tools.javac.Main;

public class HSearchShell {

	
	public String SRC_TEMP = "/tmp/src";
	public String BUILD_TEMP = "/tmp/build";
	public String JAR_TEMP = "/tmp/jar";
	
	public static void main(String[] args)  {
		new HSearchShell().createTable(args[0]);
	}
	
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
		uuid = 804677596611081216L;
		SRC_TEMP = SRC_TEMP.replace("/tmp", "/tmp/" + uuid);
		BUILD_TEMP = BUILD_TEMP.replace("/tmp", "/tmp/" + uuid);
		JAR_TEMP = JAR_TEMP.replace("/tmp", "/tmp/" + uuid);
		
		File workDirectory = new File("/tmp/" + uuid);
		workDirectory.mkdir();
		
		try {
			File file = new File(schemaFile);
			
			if(!file.exists()){
				System.err.println("Error: File " + file.getAbsolutePath()+ " not found.!");
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

			//HSearchCompiler.main(new String[]{schemaFile,SRC_TEMP});
			
			
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
			createJar();
			
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
	
	public void createJar() throws IOException {
		FileOutputStream myfileout = null;
		JarOutputStream jarout = null;

		try {
			List<JarEntry> entries = new ArrayList<JarEntry>();
			
			File javaClassDirectory = new File(BUILD_TEMP);
			List<String> classes = new ArrayList<String>();
			listAllFiles(javaClassDirectory, classes, ".class");
			String relPath = "";
			for (String source : classes) {
				File classFile = new File(source);
				if (classFile.exists() == false) {
					System.err.println("Error: File " + classFile.getAbsolutePath() + " not found.!");
					return;
				}
				relPath = classFile.getCanonicalPath().substring(BUILD_TEMP.length() + 1);
				JarEntry entry = new JarEntry(relPath);
				entries.add(entry);
			}

			File file = new File(JAR_TEMP + "/hsearch-shell.jar");
			if (file.exists() == true) {
				file.delete();
			}
			myfileout = new FileOutputStream(file);
			jarout = new JarOutputStream(myfileout);
			
			int enteriesT = entries.size();
			FileInputStream filereader  = null;
			final int buffersize = 1024;
			byte buffer[] = new byte[buffersize];
			int readcount = 0;
			
			for (int i = 0; i < enteriesT; i++) {
				jarout.putNextEntry(entries.get(i));
				filereader = new FileInputStream(classes.get(i));
				while ((readcount = filereader.read(buffer, 0, buffersize)) >= 0) {
					if (readcount > 0)
						jarout.write(buffer, 0, readcount);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		finally{
			jarout.close();
		}
	}	
}
