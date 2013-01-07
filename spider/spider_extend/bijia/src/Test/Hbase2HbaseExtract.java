package Test;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import net.hubs1.bijia.extract.ExtractCtripPrice;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;



public class Hbase2HbaseExtract {

	private Hbase2HbaseExtract(){}
	private static Hbase2HbaseExtract hbase2HbaseExtract=null;
	
	public synchronized static Hbase2HbaseExtract getInstance(){
		if(hbase2HbaseExtract == null){
			hbase2HbaseExtract = new Hbase2HbaseExtract();
		}
		return hbase2HbaseExtract;
	}
	public void select() throws Exception {
		
		HTable readTable = new HTable("download_page");
		HTable writeTable = new HTable("spider_price");
		writeTable.setAutoFlush(false);
		writeTable.setWriteBufferSize(2097152*5);
		Scan s = new Scan();
		s.setStartRow("ctrip.hotel.a".getBytes());
		s.setStopRow("ctrip.hotel.zzzzzz".getBytes());
		s.setCaching(300);
			ResultScanner ss = readTable.getScanner(s);
			int i=0;
			for(Result r:ss){
				
				
				String hotelUrl = new String(r.getRow());
				System.out.println(hotelUrl);
				String result = new String(r.getValue("base".getBytes(), "page".getBytes()));
				//logger.info(result);
				List<Map<String, String>> list = ExtractCtripPrice.extractPrice(result, hotelUrl);
				insertPrice(list,writeTable);
				i++;
			}
			System.out.println(i);
		
		writeTable.close();
		readTable.close();
	}
	public void insertPrice(List<Map<String, String>> list,HTable writeTable) throws Exception{
		//rowKey = url2RowKey(rowKey,"quna.hotel",false);
		for(Map<String, String> map:list){
			String rowKey = map.get("source_url")+"."+map.get("planCode");
			Put put = new Put(rowKey.getBytes());
			put.setWriteToWAL(false);
	        Iterator<String> it = map.keySet().iterator();
	        while(it.hasNext()) {
	            String key = (String) it.next();
				put.add("base".getBytes(), key.getBytes(), map.get(key).getBytes());

	        }
	        writeTable.put(put);
		}
	}
}
