/*
 * Copyright 2010 Bizosys Technologies Limited
 *
 * Licensed to the Bizosys Technologies Limited (Bizosys) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The Bizosys licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.bizosys.hsearch.admin;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.net.URL;

public class TableGenerator {

	private static final String PATH = "/tmp";

	public static void main(String[] args) throws Exception {

		int size = 3;
		int i = 1;
		String template = fileToString("com/bizosys/hsearch/admin/table.tmp");

		String columns = "";
		String columnDecl = "";
		String columnParams = "";
		String columnInit = "";
		String itrDecl = "";
		String itrParam = "";
		String itrInit = "";
		String nextItrParam = "";
		String builderDecl = "";
		String colDecl = "";
		String builderParam = "";
		String builderInit = "";
		String builderAdd = "";
		String builderItr = "";
		String colInit = "";
		String colSub = "";
		String colAdd = "";

		for (i = 1; i <= size; i++) {

			if (i != 1)columns = columns + ',';
			columns = columns + " C" + i;
			columnDecl += "\t\tC" + i + " c" + i + ";\n";

			if (i != 1)columnParams = columnParams + ',';
			columnParams = columnParams + " C" + i + " c" + i;

			columnInit = columnInit + "\t\t\tthis.c" + i+ " = c" + i + ";\n";
			itrDecl = itrDecl + " \t\tIterator<C"+ i + "> c" + i + "Itr = null;\n";

			if (i != 1)itrParam = itrParam + ',';
			itrParam = itrParam + " Iterator<C" + i + "> c" + i+ "Itr";

			itrInit = itrInit + "\t\t\tthis.c"+ i + "Itr = c" + i + "Itr;\n";

			if (i != 1)nextItrParam = nextItrParam + ',';
			nextItrParam = nextItrParam + " c" + i+ "Itr.next()";

			builderDecl = builderDecl + "\t\tISortedByte<C" + i+ "> c" + i + "Builder;\n";

			colDecl = colDecl+ "\t\tCollection<C" + i + "> c" + i + "L = null;\n";

			if (i != 1)builderParam = builderParam + ',';
			builderParam = builderParam + " ISortedByte<C" + i + "> c" + i;

			builderInit = builderInit + "\t\tthis.c" + i+ "Builder = c" + i + ";\n";

			colInit = colInit + "\t\tc" + i+ "L = new ArrayList<C" + i + ">();\n";

			colSub = colSub + "\t\tc" + i+ "L = c" + i + "Builder.parse(colsItr.next()).values();\n";

			builderAdd = builderAdd + "\t\tc" + i + "L.add(c" + i+ ");\n";

			if (i != 1)builderItr = builderItr + ',';
			builderItr = builderItr + " c" + i + "L.iterator()";

			colAdd = colAdd + "\t\tallCols.add(c" + i+ "Builder.toBytes(c" + i + "L));\n";

		}

		template = template.replaceAll("--COLUMNS--", columns);
		template = template.replaceAll("--COLUMN_DECLARATION--",columnDecl);
		template = template.replaceAll("--COLUMN_PARAMS--", columnParams);
		template = template.replaceAll("--COLUMN_INITALIZATION--",columnInit);
		template = template.replaceAll("--ITERATORS_DECLARATION--",itrDecl);
		template = template.replaceAll("--ITERATORS_PARAM--", itrParam);
		template = template.replaceAll("--ITERATORS_INITALIZATION--",itrInit);
		template = template.replaceAll("--NEXT_ITERATORS_PARAM--",nextItrParam);
		template = template.replaceAll("--BUILDER_DECLARATION--",builderDecl);
		template = template.replaceAll("--COLLECTION_DECLARATION--",colDecl);
		template = template.replaceAll("--BUILDER_PARAMS--", builderParam);
		template = template.replaceAll("--BUILDER_INITALIZATION--",builderInit);
		template = template.replaceAll("--COLLECTION_INITALIZATION--",colInit);
		template = template.replaceAll("--COLLECTION_SUBSTITUTION--",colSub);
		template = template.replaceAll("--BUILDER_ADDITION--", builderAdd);
		template = template.replaceAll("--BUILDER_ITERATORS--",builderItr);
		template = template.replaceAll("--COLUMN_SIZE--",new Integer(size).toString());
		template = template.replaceAll("--COLUMN_ADDITION--", colAdd);

		String fileName = PATH + "/Table" + size + ".java";
		downloadToFile(template.getBytes(),new File(fileName));
		System.out.println(template);
	}

	public static String fileToString(String fileName) {

		File aFile = getFile(fileName);
		BufferedReader reader = null;
		InputStream stream = null;
		StringBuilder sb = new StringBuilder();
		try {
			stream = new FileInputStream(aFile);
			reader = new BufferedReader(new InputStreamReader(stream));
			String line = null;
			String newline = "\n";
			while ((line = reader.readLine()) != null) {
				if (line.length() == 0)
					continue;
				sb.append(line).append(newline);
			}
			return sb.toString();
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		} finally {
			try {
				if (null != reader)
					reader.close();
			} catch (Exception ex) {
				ex.printStackTrace(System.err);
			}
			try {
				if (null != stream)
					stream.close();
			} catch (Exception ex) {
				ex.printStackTrace(System.err);
			}
		}
	}

	public static File getFile(String fileName) {
		File aFile = new File(fileName);
		if (aFile.exists())
			return aFile;

		aFile = new File("/" + fileName);
		if (aFile.exists())
			return aFile;

		aFile = new File("conf/" + fileName);
		if (aFile.exists())
			return aFile;

		aFile = new File("resources/" + fileName);
		if (aFile.exists())
			return aFile;

		try {
			URL resource = TableGenerator.class.getClassLoader().getResource(
					fileName);
			if (resource != null)
				aFile = new File(resource.toURI());
		} catch (URISyntaxException ex) {
			throw new RuntimeException(ex);
		}

		if (aFile.exists())
			return aFile;

		throw new RuntimeException("FileResourceUtil > File does not exist :"
				+ fileName);
	}

	public static void downloadToFile(byte[] bytes, File file) {
		try {
			DataOutputStream out = new DataOutputStream(
					new BufferedOutputStream(new FileOutputStream(file)));
			out.write(bytes);
			out.flush();
			out.close();
			System.out.println(file.getAbsolutePath()
					+ "  is written sucessfully.");

		} catch (IOException e) {
			e.printStackTrace(System.err);
			System.exit(1);
		}
	}

}