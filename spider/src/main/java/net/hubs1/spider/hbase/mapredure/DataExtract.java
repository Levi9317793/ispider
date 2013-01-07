package net.hubs1.spider.hbase.mapredure;

import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.jsoup.Jsoup;

import net.hubs1.spider.core.Command;
import net.hubs1.spider.core.CommandParse;
import net.hubs1.spider.core.RunCommand;
import net.hubs1.spider.store.HbaseClient;
import net.hubs1.spider.store.KVBean;

public class DataExtract {
	private static Logger log=Logger.getLogger(DataExtract.class);
	private Map<String,List<Command>> commands=null;
	private boolean initOK=false;
	public  DataExtract(String format,boolean isLocal){
		if(format!=null){
			if(isLocal){
				commands=CommandParse.parse(format);
			}else{
				Configuration conf = new Configuration();
				try{
					FileSystem fsFileSystem = FileSystem.get(conf);
					FSDataInputStream fis=fsFileSystem.open(new Path(format));
					
					LineNumberReader lnr=new LineNumberReader(new InputStreamReader(fis));
					List<String> sb=new ArrayList<String>();
					String line=null;
					while( (line=lnr.readLine())!=null){
						sb.add(line);
					}
					commands=CommandParse.parse(sb);
					initOK=true;
				}catch(Exception e){
					log.error(e);
					initOK=false;
				}
			}
		}else{
			commands=CommandParse.parse(DataExtract.class.getResource("default.txt").getPath());
			initOK=true;
		}
	}
	
	public Map<String,Object> getConfig(){
		try {
			Map<String, Object> initMap = RunCommand.executeBlock(commands.get("init"),commands);
			return initMap;
		} catch (Exception e) {
			log.error(e);
		}
		return null;
	}
	
	public Map<String,Object> extract(String html,String rowkey){
		log.info("start extract the source date rowke is:["+rowkey+"]");
		try {
			
			Map<String, Object> runGMap=new LinkedHashMap<String,Object>();
			runGMap.put("sourceRowkey", rowkey);
			
			Map<String, Object> runMap=new HashMap<String,Object>();
			runMap.put("current", Jsoup.parse(html));
			RunCommand.execute(commands.get("run"),commands,runMap,runGMap);
			runMap.remove("current");
			runMap.remove("sys_test_result");
			return runMap;
		} catch (Exception e) {
			log.error("fail,the source data rowkey is:["+rowkey+"]",e);
		}
		return new HashMap<String,Object>() ;
	}
	
	
	public Map<String,Object> extract(){
		try {
			Map<String, Object> runMap=new LinkedHashMap<String,Object>();
			Map<String, Object> runGMap=new LinkedHashMap<String,Object>();
			RunCommand.execute(commands.get("run"),commands,runMap,runGMap);
			runMap.remove("current");
			return runMap;
		} catch (Exception e) {
			log.error(e);
		}
		return null;
	}
	
	public static void main(String[] args) {
		HbaseClient hc=HbaseClient.getInstance();
		List<KVBean> list=hc.scan("download_page","base", "tc.hotel.HotelInfo-22948", "tc.hotel.HotelInfo-22948", "page");
		
		DataExtract hm=new DataExtract(DataExtract.class.getClassLoader().getResource("test.txt").getPath(),true);
		for(KVBean kv:list){
			Map<String,Object> mp=hm.extract(kv.getValue(),kv.getKey());
				for(Map.Entry<String,Object> entry:mp.entrySet()){
					System.out.println(entry.getKey()+"="+(entry.getValue()!=null?entry.getValue().toString():""));
				}
//			for (Map.Entry<String,Object> entry:mp.entrySet()) {
//  				if(entry.getValue() instanceof Collection){
//  					Collection<Map<String,Object>> maps=null;
//  					try{
//  						maps=(Collection<Map<String,Object>>)entry.getValue();
//  					}catch(ClassCastException e){
//  						log.error(e);
//  					}
//  					
//  					for(Map<String,Object> map:maps){
//  						String rk=(String)map.remove("rowkey");
//  						System.out.println("====="+rk+"======");
//  						for(Map.Entry<String, Object> attribute:map.entrySet()){
//  							try{
//	  							System.out.println(attribute.getKey()+"="+(String)attribute.getValue());
//  							}catch(Exception e){
//  								System.out.println("====="+attribute.getKey()+","+attribute.getValue());
//  							}
//  						}
//  					}
//  				}
//  			}
		}
	}

	public boolean isInitOK() {
		return initOK;
	}  
}
