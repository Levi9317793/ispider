package net.hubs1.bijia.extract;

import java.security.MessageDigest;

public class Until {
	public static String encrypt(String input) throws Exception{
		MessageDigest md5 = MessageDigest.getInstance("MD5"); 
		byte[] bytes = md5.digest(input.getBytes());
		StringBuilder sb =new StringBuilder();
		for(int i=0;i<bytes.length;i++){   
	        int v=bytes[i]&0xff;  
	        if(v<16){  
	            sb.append(0);  
	        }  
	        sb.append(Integer.toHexString(v));  
	    } 
		return sb.toString();
	}
	public  static void main(String[] args)throws Exception
    {
		for(int i=1;i<100;i++){
			
			System.out.println(encrypt(Integer.toString(i)).length());
			
		}
    }
}
