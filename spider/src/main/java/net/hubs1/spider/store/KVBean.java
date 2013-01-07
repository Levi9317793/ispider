package net.hubs1.spider.store;

import java.io.Serializable;

public class KVBean implements Serializable{
	private String key = null;
	private byte[] value = null;
	private boolean isStream=false;
	
	public KVBean(){
		
	}
	
	public KVBean(String key,String value){
		this.key=key;
		this.value=value!=null?value.getBytes():null;
	}
	
	public KVBean(String key,byte[] value){
		this.key=key;
		this.value=value;
	}
	
	public KVBean(String key,byte[] value,boolean vISStream){
		this.key=key;
		this.value=value;
		this.isStream=vISStream;
	}
	
	public KVBean(byte[] key,byte[] value){
		this.key=new String(key);
		this.value=value;
	}
	
	public String getValue() {
		return this.value!=null?new String(this.value):null;
	}

	public void setValue(String value) {
		this.value = value!=null?value.getBytes():null;
	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}
	
	public byte[] getValueByte(){
		return this.value;
	}
	
	public byte[] getKeyByte(){
		return this.key!=null?this.key.getBytes():null;
	}

	public boolean isStream() {
		return isStream;
	}

	public void setStream(boolean isStream) {
		this.isStream = isStream;
	}
}
