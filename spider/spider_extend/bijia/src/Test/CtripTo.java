package Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

public class CtripTo {

	public static void main(String[] args) throws IOException{
		HTable readTable = new HTable("download_page");

		Scan s = new Scan();
		s.setStartRow("qunar.bijia.a".getBytes());
		s.setStopRow("qunar.bijia.zzzzzzzzzzzz".getBytes());
		s.setCaching(300);
			ResultScanner ss = readTable.getScanner(s);
			int i=0;
			for(Result r:ss){
				i++;
				if(i==1000)i++;
			}
		System.out.println(i);	
		
	}
}
