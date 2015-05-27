package com.es.offline;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.log4j.Logger;


public class Initialize {
	
	
private static Logger _LOG = Logger.getLogger(Initialize.class);

	
public static final  String timestamp="@fields.timestamp";
	
public static   String prefix;
public static   String eshost;
public static   String esport;

public static  String esname;

public static  String query;
public static  String field;
public static  String[] types;
//public static  String indextarget;
public static  String estarget;
public static  String estargetname;
public static  String estargetport;

public static  int size;
public static  int bulksize;



//public final static String user;

//public final static String password;


//public static final Properties prop;

public static final int TIMEOUT=60000;


static {
	

		// DateTimeZone.setDefault(DateTimeZone.forID("Asia/Shanghai"));  

	
		 Properties prop = new Properties();
	   String path = Thread.currentThread().getContextClassLoader().getResource("").getPath(); 
	   _LOG.info(path);
	   path=path+"init.properties";
		
		_LOG.info("path "+path);
	   
		
		try {
			prop.load(new FileInputStream(path));
		} catch ( IOException e) {
			
			throw new RuntimeException(e);
		}
		prefix=prop.getProperty("prefix");
		eshost=prop.getProperty("eshost");
		estarget=prop.getProperty("estarget");
		
		
		esport=prop.getProperty("esport");
		estargetport=prop.getProperty("estargetport");
		
		
		esname=prop.getProperty("esname");
		estargetname=prop.getProperty("estargetname");
		
		
		query=prop.getProperty("query");
		field=prop.getProperty("field");
		String typestr=prop.getProperty("types");
		types=prop.getProperty("types").equals("") ? new String[0] : typestr.split(",");
		//indextarget=prop.getProperty("indextarget");
		size=Integer.parseInt( prop.getProperty("size"));
		bulksize=Integer.parseInt( prop.getProperty("bulksize"));
		
		
		_LOG.info(prefix);
		_LOG.info(eshost);
		_LOG.info(esname);
		
		//user=prop.getProperty("user");
		
		//password=prop.getProperty("password");


}






}
