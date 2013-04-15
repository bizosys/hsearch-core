package com.bizosys.hsearch.util.conf;

/** Base class for things that may be configured with a {@link Configuration}. */
public class Configured implements Configurable {

  private Configuration conf; 

  /** Construct a Configured. */
  public Configured(Configuration conf) {
	  this.setConf(conf); 
  }

  // inherit javadoc
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  // inherit javadoc
  public Configuration getConf() {
    return conf;
  }

}
