package com.cloudera.services.hbase.client;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

public class ConfigUtils {
	
	public static final Properties serviceProps = new Properties();
	
	static {
		try {
			 serviceProps.load(ConfigUtils.class.getClassLoader().getResourceAsStream("config.properties"));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}
}