package com.adamfei.util;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.Serializable;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * 加载properties
 * @author adam
 * @since 2016.3.3
 */
public class PropertiesLoader implements Serializable {
	
	/**
	 * 根据路径加载配置文件
	 * @param filePath
	 * @return
	 */
	public static Map<String, String> load(String filePath){
		
		Map<String, String> resultMap = new HashMap<String, String>();
		
		Properties props = new Properties();
		
		try {
			InputStream in = PropertiesLoader.class.getClassLoader().getResourceAsStream(filePath);
			props.load(in);
            Enumeration en = props.propertyNames();
            while (en.hasMoreElements()) {
            	String key = (String) en.nextElement();
                String Property = props.getProperty (key);
                resultMap.put(key, Property);
            }
        } catch (Exception e) {
        	e.printStackTrace();
        }
		
		
		return resultMap;
	}
	
}