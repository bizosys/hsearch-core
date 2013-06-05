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
			Manifest manifest = new Manifest();
			manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
			manifest.getMainAttributes().put(Attributes.Name.MAIN_CLASS, "com.bizosys.hsearch.treetable.example.impl.Webservice");
			JarOutputStream target = new JarOutputStream(new FileOutputStream(JAR_TEMP + "/hsearch-shell.jar"), manifest);
			
			System.out.println("Building Jar : ");
			createJar(new File(BUILD_TEMP), target);
			target.close();
			
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
	
}
