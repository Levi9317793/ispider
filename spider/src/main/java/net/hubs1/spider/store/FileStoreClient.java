package net.hubs1.spider.store;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;

import org.apache.log4j.Logger;

import net.hubs1.spider.core.DBClient;
import net.hubs1.spider.core.config.SpiderContext;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

/**
 * 将数据保持至文件。
 * @author 董明
 *
 */
public class FileStoreClient implements DBClient {
	private static Logger log=Logger.getLogger(FileStoreClient.class);
	private FileOutputStream fout=null;
	
	public FileStoreClient(){
		String file=SpiderContext.getConfig().getSaveFilePath();
		if(file==null){
			file=FileStoreClient.class.getClassLoader().getResource("/").getPath();
			file=file+"/spider_out.db";
		}
		File f=new File(file+"/spider_out.db");
		
		try {
			this.fout=new FileOutputStream(f);
		} catch (FileNotFoundException e) {
			log.error(" init file error ",e);
		}
		
	}
	
	@Override
	public boolean writeData(String tableName, RecordsBean... attributes) {
		JSONObject jo=new JSONObject();
		try {
			for(RecordsBean rb:attributes){
				jo.put("rowkey", new String(rb.getRowkey()));
				jo.put("family", new String(rb.getFamily()));
				JSONArray array=new JSONArray();
				for(KVBean kv:rb.getAttributes()){
					JSONObject jskv=new JSONObject();
					jskv.put("column", kv.getKey());
					jskv.put("value",  kv.isStream()?kv.getValueByte():kv.getValue());
					array.add(jskv);
				}
				jo.put("columns", array);
			}
			fout.write(jo.toString().getBytes());
			fout.flush();
			return true;
		} catch (IOException e) {
			log.error(e);
		}
		return false;
	}

	@Override
	public List<KVBean> scan(String tableName, String family,
			String rowkeyStart, String rowkeyEnd, String column) {
		return HbaseClient.getInstance().scan(tableName, family, rowkeyStart, rowkeyEnd, column);
		
	}

	@Override
	public void commit(String tableName) {
		// TODO Auto-generated method stub
	}

	@Override
	public List<RecordsBean> scan(String tableName, String family,
			String rowkeyStart, String rowkeyEnd, String[] columns) {
		return HbaseClient.getInstance().scan(tableName, family, rowkeyStart, rowkeyEnd, columns);
	}

	@Override
	public RecordsBean get(String tableName, String family, String rowkey,
			String[] columns) {
		
		return HbaseClient.getInstance().get(tableName, family, rowkey, columns);
	}

	@Override
	public boolean delete(String tableName, String rowkey) {
		return HbaseClient.getInstance().delete(tableName, rowkey);
	}

}
