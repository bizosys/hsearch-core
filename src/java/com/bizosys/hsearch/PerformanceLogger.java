package com.bizosys.hsearch;

import org.apache.log4j.Logger;

public class PerformanceLogger {
	
	public static class L {
		public void info(String msg) {
			System.out.println(msg);
		}
		
		public boolean isInfoEnabled() {
			return true;
		}
	}
	
	public static L l = new L();
	
	//public static Logger l = Logger.getLogger(PerformanceLogger.class.getName());
}
