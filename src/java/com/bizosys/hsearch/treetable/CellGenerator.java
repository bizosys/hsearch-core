package com.bizosys.hsearch.treetable;

import java.util.List;

import com.oneline.util.FileReaderUtil;

public class CellGenerator {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		
		int cells = 11;
		String template = FileReaderUtil.toString("com\\bizosys\\hsearch\\treetable\\CellN.txt");
		template = replaceClassNMinus1(template, cells);
		template = replaceClass(template, cells);
		template = replaceCellN(template, cells);
		template = replaceSortedDecl(template, cells);
		template = replaceSortedAssigner(template, cells);
		template = replaceSorters(template, cells);
		template = replaceSorterInstMinus1(template, cells);
		template = replaceSorterInst(template, cells);
		template = replaceValMinus1(template, cells);
		template = replaceLastArg(template, cells);
		template = cellMinus1(template, cells);
		template = replaceParamsN(template, cells);
		
		System.out.println(template);
		
	}
	
	public static String replaceClass(String clazzText, int size) {
		return clazzText.replaceAll("--CLASS--", "Cell" + size);
	}
	
	public static String replaceClassNMinus1(String clazzText, int size) {
		
		String cell = "Cell" + (size-1) + "<" ;
		
		for ( int i=2; i<size; i++) {
			if ( i != 2 ) cell = cell + ',';
			cell = cell + " K" + i;
		}	
		cell = cell + ",V>";
		return clazzText.replaceAll("--CELLN-1--", cell);
		
	}
	
	public static String replaceCellN(String clazzText, int size) {
		
		String cell = "Cell" + (size) + "<" ;
		
		for ( int i=1; i<size; i++) {
			if ( i != 1 ) cell = cell + ',';
			cell = cell + " K" + i;
		}	
		cell = cell + ",V>";
		return clazzText.replaceAll("--CELLN--", cell);
		
	}	
	
	public static String replaceSortedDecl(String clazzText, int size) {
		String decl = "";
		
		for ( int i=2; i<size; i++) {
			decl = decl + "public ISortedByte<K" + i + "> k" + i + "Sorter = null;\n\t";
		}	
		return clazzText.replaceAll("--SORTER_DECL--", decl);
	}
	
	public static String replaceSortedAssigner(String clazzText, int size) {
		String decl = "";
		
		for ( int i=1; i<size; i++) {
			decl = decl + "this.k" + i + "Sorter = k" + i + "Sorter;\n\t\t";
		}	
		return clazzText.replaceAll("--SORTER_ASSIGNER--", decl);
	}	
	
	public static String replaceSorters(String clazzText, int size) {
		String decl = "";
		
		for ( int i=1; i<size; i++) {
			if ( i != 1) decl = decl + ",";
			decl = decl + "ISortedByte<K" + i + "> k" + i + "Sorter";
		}	
		return clazzText.replaceAll("--SORTERS--", decl);
	}	
	
	public static String replaceSorterInstMinus1(String clazzText, int size) {
		String decl = "";
		
		for ( int i=2; i<size; i++) {
			if ( i != 2) decl = decl + ",";
			decl = decl + "k" + i + "Sorter";
		}	
		return clazzText.replaceAll("--SORTERN-1_INST--", decl);
	}
	
	public static String replaceSorterInst(String clazzText, int size) {
		String decl = "";
		
		for ( int i=1; i<size; i++) {
			if ( i != 1) decl = decl + ",";
			decl = decl + "k" + i + "Sorter";
		}	
		return clazzText.replaceAll("--SORTER_INST--", decl);
	}
	
	public static String replaceValMinus1(String clazzText, int size) {
		String decl = "";
		
		for ( int i=2; i<size; i++) {
			if ( i != 2) decl = decl + ",";
			decl = decl + "k" + i;
		}	
		decl = decl + ", v";
		return clazzText.replaceAll("--VAL_N-1--", decl);
	}	
	
	public static String replaceLastArg(String clazzText, int size) {
		return clazzText.replaceAll("--LAST_ARG--", "K" + (size-1));
	}	

	public static String cellMinus1(String clazzText, int size) {
		return clazzText.replaceAll("--CellN-1--", "cell" + (size-1));
	}	
	
	public static String replaceParamsN(String clazzText, int size) {
		String decl = "";
		
		for ( int i=1; i<size; i++) {
			if ( i != 1) decl = decl + ",";
			decl = decl + ("K" + i + " k" + i);
		}	
		decl = decl + ", V v";
		return clazzText.replaceAll("--PARAM_N--", decl);
	}	
		

}