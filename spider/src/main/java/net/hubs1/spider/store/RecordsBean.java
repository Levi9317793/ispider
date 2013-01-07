package net.hubs1.spider.store;

import java.util.HashMap;
import java.util.Map;


public class RecordsBean {
	private byte[] rowkey=null;
	private byte[] family=null;
	private KVBean[] attributes=null;
	
	private Map<String,byte[]> map=null;
	
	public RecordsBean(){
	}
	
	
	public RecordsBean(String family,String rowkey){
		this.rowkey=rowkey.getBytes();
		this.family=family.getBytes();
	}
	
	public RecordsBean(String family,String rowkey,KVBean... attributes){
		this.rowkey=rowkey.getBytes();
		this.family=family.getBytes();
		this.attributes=attributes;
	}
	
	public byte[] getRowkey() {
		return rowkey;
	}
	
	public Map<String,byte[]> attribute2Map(){
		if(map==null&&attributes!=null){
			map=new HashMap<String, byte[]>();
			for(KVBean kv:attributes){
				map.put(kv.getKey(), kv.getValueByte());
			}
		}
		return map;
	}
	
	public String getRowkeyStr(){
		return new String(rowkey);
	}
	
	public void setRowkey(byte[] rowkey) {
		this.rowkey = rowkey;
	}
	public byte[] getFamily() {
		return family;
	}
	public void setFamily(byte[] family) {
		this.family = family;
	}
	public KVBean[] getAttributes() {
		return attributes;
	}
	public void setAttributes(KVBean[] attributes) {
		this.attributes = attributes;
	}
	
}
