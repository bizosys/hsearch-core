package com.bizosys.hsearch.util;

import com.bizosys.hsearch.util.conf.Configuration;

public class HSearchConfig {

	private static HSearchConfig instance = null;
	public  static final HSearchConfig getInstance() {
		if ( null != instance) return instance;
		synchronized (HSearchConfig.class.getName()) {
			if ( null != instance) return instance;
			instance = new HSearchConfig();
		}
		return instance;
	}
	
	Configuration config = null;
	private HSearchConfig() {
		this.config = new Configuration();
	}
	
	public final Configuration getConfiguration() {
		return config;
	}
	
	public static void main(String[] args) throws Exception {
		System.out.println ( HSearchConfig.getInstance().getConfiguration().get("threads.count") );
	}
	
}
