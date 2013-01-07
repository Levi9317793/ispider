package net.hubs1.bijia.url;

import java.io.IOException;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

public class CtripDianping {

	public static void main(String[] args){
		try {
			HTable table = new HTable("download_url");
			Scan s = new Scan();
			s.setCaching(10000);
			s.setStartRow("ctrip.hotel.a".getBytes());
			s.setStopRow("ctrip.hotel.zzzzzzzzzzzz".getBytes());
			table.setAutoFlush(false);
			String rowKey = "ctrip.dianping.dianping.";
			String urlPreFix = "/hotel/dianping/";
			String urlPostFix = ".html";
			ResultScanner ss = table.getScanner(s);
			int i=0;
			for(Result r:ss){
					String hotelUrl = new String(r.getRow());
					System.out.println(hotelUrl);
					hotelUrl = hotelUrl.replaceAll("\\D", "");
					System.out.println(Integer.parseInt(hotelUrl));
//					Put put = new Put((rowKey+hotelUrl).getBytes());
//					put.setWriteToWAL(false);
//					put.add("base".getBytes(), "source".getBytes(), "ctrip".getBytes());
//					put.add("base".getBytes(), "url".getBytes(),
//							(urlPreFix+hotelUrl+urlPostFix).getBytes());
//					try {
//						table.put(put);
//					} catch (IOException e) {
//						// TODO Auto-generated catch block
//						e.printStackTrace();
//					}
					i++;
			}
			System.out.println(i);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
