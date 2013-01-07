package net.hubs1.spider.store;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.log4j.Logger;

import net.hubs1.spider.core.DBClient;

public class HbaseClient implements DBClient{
	private Configuration config=null;
	private HBaseAdmin ha=null;
	private HTablePool tablePool=null;
	private static Logger log=Logger.getLogger(HbaseClient.class);
	private static HbaseClient hc=new HbaseClient();
	
	public static HbaseClient getInstance(){
		return hc;
	}
	
	private HbaseClient(){
		config=HBaseConfiguration.create();
		try{
			ha=new HBaseAdmin(config);
		}catch(Exception e){
			log.error(e);
		}
		tablePool=new HTablePool(config, 10);
	}
	
	@Override
	public boolean writeData( String tableName,RecordsBean... attributes)  {
		HTableInterface table=null;
		try{
			table=tablePool.getTable(tableName.getBytes());
			for(RecordsBean record:attributes){
				if(record==null) continue;
				Put update=new Put(record.getRowkey());
				
				for(KVBean attribute:record.getAttributes()){
					if(attribute==null) continue;
					update.add(record.getFamily(),attribute.getKeyByte(), attribute.getValueByte());
				}
				table.put(update);
			}
			table.flushCommits();
			return true;
		}catch(Exception e){
			log.error("write data error:",e);
			int index=0;
			do{
				try {
					Thread.sleep(1000*60*5);//5分钟重试一次
				} catch (InterruptedException e1) {}
				connectionTest();
				if(writeData(tableName,attributes)) break;
			}while(index++<20);
		}finally{
			if(table!=null)
				try {
					 table.close();
				} catch (IOException e) {e.printStackTrace();}
		}
		return false;
	}
	
	public synchronized void connectionTest(){
		log.error("start test hbase status.");
		HTableInterface table=null;
		boolean isOK=false;
		try{
			table=tablePool.getTable("spider_test_avalible");
			Get get=new Get("sate".getBytes());
			Result r=table.get(get);
			if(!r.isEmpty()) isOK=true;
		}catch(Exception e){
			isOK=false;
		}finally{
			try {table.close();} catch (IOException e) {e.printStackTrace();}
		}
		if(!isOK){
			log.error("the hbase status is't connection,init again");
			hc=new HbaseClient();
			log.error("init was finished!");
		}else{
			log.error("the hbase status is avalible");
		}
	}
	
	public boolean isExist(String tableName) {
		try{
			return ha.tableExists(tableName);
		}catch (IOException e) {
			log.error(e);
			return false;
		}
	}
	
	
	public List<KVBean> scan(String tableName,String family,String rowkeyStart,String rowkeyEnd,String column){
		HTableInterface table=tablePool.getTable(tableName.getBytes());
		Scan scan=new Scan();
//		scan.setCaching(100);
		scan.setBatch(100);
		if(column!=null) scan.addColumn(family.getBytes(),column.getBytes());
		if(rowkeyStart!=null)scan.setStartRow(rowkeyStart.getBytes());
		if(rowkeyEnd!=null)scan.setStopRow(rowkeyEnd.getBytes());
		
		List<KVBean>  list=new ArrayList<KVBean>();
		try{
			ResultScanner r=table.getScanner(scan);
			Iterator<Result> ite=r.iterator();
			while(ite.hasNext()){
				Result result=ite.next();
				String rowkey=new String(result.getRow());
				String columnValue=new String(result.getValue(family.getBytes(), column.getBytes()));
				
				list.add(new KVBean(rowkey,columnValue));
			}
		}catch(Exception e){
			log.error(e);
			return new ArrayList<KVBean>();
		}finally{
			try {table.close();} catch (IOException e) {e.printStackTrace();}
		}
		return list;
	}
	
	public Map<String,byte[]> get(String tableName,String family,String rowkey){
		return get(tableName, family, rowkey, -1);
	}
	
	private Map<String,byte[]> get(String tableName,String family,String rowkey,int versions){
		HTableInterface table=tablePool.getTable(tableName.getBytes());
		Get get=new Get(rowkey.getBytes());
		try {
			if(versions!=-1) get.setMaxVersions(versions);
			Result r=table.get(get);
			if(!r.isEmpty()){
				Map<String,byte[]> map=new HashMap<String,byte[]>();
				map.put("rowkey", r.getRow());
				List<KeyValue> kvList=r.getColumn(family.getBytes(), null);
				if(kvList!=null){
					for(KeyValue kv:kvList){
						map.put(kv.getKeyString(),kv.getValue());
					}
				}
				return map;
			}
		} catch (IOException e) {
			log.error(e);
		}finally{
			try {table.close();} catch (IOException e) {e.printStackTrace();}
		}
		return null;
	}

	public void test() throws IOException{
		List<KVBean> list=this.scan("download_url", "base", "qunar.bijia.a","qunar.bijia.z", "source");
		System.out.println(list.size());
//		Map<String,Integer> map=new LinkedHashMap<String, Integer>();
//		int count=0;
//		for(KVBean kv:list){
//			RecordsBean rb=this.get("spider_prop_en", "prop_base", kv.getKey(),new String[]{"country","city"});
//			String country=new String(rb.attribute2Map().get("country")).trim();
//			String city=new String(rb.attribute2Map().get("city")).trim();
//			
//			String key=country+","+city;
//			Integer r=map.get(key);
//			map.put(key, r==null?1:(r+1));
//			if(kv.getValue().contains("普吉岛")){
//					++count;
//			}
			
			
//		}
//		System.out.println(count);
//		FileWriter fw=new FileWriter(new File("d:\\count_en.txt"));
//		for(Map.Entry<String, Integer> me:map.entrySet()){
//			fw.write(me.getKey());
//			fw.write(",");
//			fw.write(me.getValue().toString());
//			fw.write("\r\n");
//		}
//		fw.close();
		
	}
	
	public static void main(String[] args) throws IOException {
		HbaseClient bc=new HbaseClient();
		bc.test();
		Calendar c= Calendar.getInstance();
		c.setTimeInMillis(1353882381886L);
		
		System.out.println(c.toString());
//		bc.commit("download_url");
		//System.out.println("http://www.booking.com/hotel/us/inter-continental.zh-cn.html?sid=8d9ae307e969119fb3ffee70f5845d2c;dcid=1".replaceAll("\\.com", ""));
//		
//		
		
	}

	@Override
	public void commit(String tableName) {
		try {
			tablePool.getTable(tableName.getBytes()).flushCommits();
		} catch (IOException e) {
			log.error(e);
		}
	}

	@Override
	public List<RecordsBean> scan(String tableName, String family,
			String rowkeyStart, String rowkeyEnd, String[] columns) {
		
		HTableInterface table=tablePool.getTable(tableName.getBytes());
		Scan scan=new Scan();
//		scan.setCaching(10);
		if(columns!=null&&columns.length>0){
			for(String column:columns){
				scan.addColumn(family.getBytes(),column.getBytes());
			}
		}
		if(rowkeyStart!=null)scan.setStartRow(rowkeyStart.getBytes());
		if(rowkeyEnd!=null)scan.setStopRow(rowkeyEnd.getBytes());
		
		List<RecordsBean>  list=new ArrayList<RecordsBean>();
		try{
			ResultScanner r=table.getScanner(scan);
			Iterator<Result> ite=r.iterator();
			while(ite.hasNext()){
				Result result=ite.next();
				String rowkey=new String(result.getRow());
				
				RecordsBean rb=new RecordsBean(family, rowkey);
				NavigableMap<byte[],byte[]> rsMap=result.getFamilyMap(family.getBytes());
				
				Set<Map.Entry<byte[],byte[]>> kvSet=rsMap.entrySet();
				
				KVBean[] attributes=new KVBean[kvSet.size()];
				
				int i=0;
				for(Map.Entry<byte[],byte[]> kv:kvSet){
					KVBean kvb=new KVBean(kv.getKey(),kv.getValue());
					attributes[i++]=kvb;
				}
				
				rb.setAttributes(attributes);
				list.add(rb);
			}
		}catch(Exception e){
			log.error(e);
			return new ArrayList<RecordsBean>();
		}finally{
			try {table.close();} catch (IOException e) {e.printStackTrace();}
		}
		return list;
	}

	@Override
	public RecordsBean get(String tableName, String family, String rowkey,
			String[] columns) {
		HTableInterface table=tablePool.getTable(tableName.getBytes());
		Get get=new Get(rowkey.getBytes());
//		scan.setCaching(10);
		if(columns!=null&&columns.length>0){
			for(String column:columns){
				get.addColumn(family.getBytes(),column.getBytes());
			}
		}
		try{
			Result result=table.get(get);
			
			if(result!=null&&!result.isEmpty()){
				String rk=new String(result.getRow());
				
				if(rk==null) return null;
				
				RecordsBean rb=new RecordsBean(family, rk);
				NavigableMap<byte[],byte[]> rsMap=result.getFamilyMap(family.getBytes());
				
				Set<Map.Entry<byte[],byte[]>> kvSet=rsMap.entrySet();
				
				KVBean[] attributes=new KVBean[kvSet.size()];
				
				int i=0;
				for(Map.Entry<byte[],byte[]> kv:kvSet){
					KVBean kvb=new KVBean(kv.getKey(),kv.getValue());
					attributes[i++]=kvb;
				}
				
				rb.setAttributes(attributes);
				return rb;
			}
		}catch(Exception e){
			log.error(e);
			return null;
		}finally{
			try {table.close();} catch (IOException e) {e.printStackTrace();}
		}
		return null;
	}

	@Override
	public boolean delete(String tableName, String rowkey) {
		HTableInterface table=tablePool.getTable(tableName.getBytes());
		Delete d=new Delete(rowkey.getBytes());
		try{
			table.delete(d);
			return true;
		}catch(Exception e){
			log.error(e);
			return false;
		}finally{
			try {table.close();} catch (IOException e) {e.printStackTrace();}
		}
	}
}
