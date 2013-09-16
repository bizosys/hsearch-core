package com.bizosys.hsearch.util;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import com.bizosys.hsearch.federate.FederatedSearch;

public class ShutdownCleanup {
	
	private static ShutdownCleanup singleton = null;
	
	public static ShutdownCleanup getInstance() {
		if ( null != singleton) return singleton;
		
		synchronized (ShutdownCleanup.class.getName()) {
			if ( null != singleton) return singleton;
			singleton = new ShutdownCleanup();
		}
		return singleton;
	}
	
	Set<ExecutorService> services = new HashSet<ExecutorService>();
	public void addExectorService(ExecutorService es) {
		services.add(es);
	}
	
	private ShutdownCleanup() {
		
    	System.out.println("ShutdownCleanup initialized");
		Runtime.getRuntime().addShutdownHook(new Thread() {
		    public void run() { 
		    	System.out.println("Cleaning up on shutdown");
		    	for (ExecutorService ex : services) {
		    		try {
		    			if ( null != ex) ex.shutdown();
		    		} catch (Exception e) {
		    			System.err.println("Warning : Shutdown Failure - " + e.getMessage());
		    		}
				}
		    	 
		    }
		});		
		this.addExectorService(FederatedSearch.getExecutorService());
	}

}
