package net.hubs1.spider.download;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import net.hubs1.spider.core.Command;
import net.hubs1.spider.core.CommandParse;
import net.hubs1.spider.core.DBClient;
import net.hubs1.spider.core.action.TaskAction;
import net.hubs1.spider.core.action.TaskInfo;
import net.hubs1.spider.core.config.SpiderConfig;
import net.hubs1.spider.core.config.SpiderContext;
import net.hubs1.spider.rabbit.MQMessage;
import net.hubs1.spider.rabbit.RabbitMQClient;
import net.hubs1.spider.store.HbaseClient;
import net.hubs1.spider.store.KVBean;

/**
 * @author 董明
 * 
 */
public class DownloadManager implements DownloadStatus{
	private static Logger log=Logger.getLogger(DownloadManager.class);
	private RabbitMQClient rabbit=RabbitMQClient.getInstance();
	private ExecutorService threadPools = null;	
	
	private Set<String> taskList=new HashSet<String>();
	
	private static DBClient db=HbaseClient.getInstance();//这里引用db，只是为了尽早初始化，以便在启动服务器端时就发现问题。
	
	private String publicQueue=null;
	private int threadNum=100;
	private int usedThread=0;
	private static long sleepTime=1000*10;//10秒
	private FileSystem dfsFileSystem=null;
	
	public DownloadManager(SpiderConfig config){
		this.publicQueue=config.getPublicQueueName();
		threadPools=Executors.newFixedThreadPool(config.getDownloadThreadNum());
		initHdfsFileSystem(true);
	}
	
	private void initHdfsFileSystem(boolean exit){
		Configuration conf = new Configuration();
		try {
			 this.dfsFileSystem = FileSystem.get(conf);
		} catch (IOException e) {
			log.error(" =====don't init hdfs file system=======");
			if(exit)System.exit(1);
		}
	}
	
	public void receviceQueueInfo(){
		long responTag=-1;
		while(true){
			MQMessage<KVBean> message=null;
			if(responTag!=-1) rabbit.rejectMessage(responTag, this.publicQueue, false);
			sleep(sleepTime);
			String taskID=null;
			try {
				if(usedThread>=threadNum){
					sleepTime=1000*60*10;//如果线程满了，则10分钟检查一次
					continue;
				}
				
				log.debug(" start receive public message");
				responTag=-1;
				message=rabbit.receivePubMessage();
				if(message==null) continue;
	
				responTag=message.getResponseTag();
				
				KVBean kv=message.getMessage();
				log.debug("public message is:"+kv.getKey()+"="+kv.getValue());
				
				int index=kv.getKey().indexOf(":");
				
				if(index<1||kv.getValue()==null){
					log.error("not found taskID or the format path is not set!that is required. cancle this task");
					continue;
				}
				
				String type=kv.getKey().substring(0,index);
				taskID=kv.getKey().substring(index+1);
				String formatPath=kv.getValue();
				log.info("receive task from "+publicQueue+",the type is"+type+" taskID:"+taskID+" formatPath is:"+formatPath);
				
				synchronized (taskList) {
					if(this.taskList.contains(taskID)){
						log.warn(" the task was already exists !");
						continue;
					}else{
						this.taskList.add(taskID);
					}
				}
				
				List<String> formats=readHdfsFile(formatPath);
				Map<String,List<Command>> commandMap=null;
				if(formats!=null)
					commandMap=CommandParse.parse(formats);
				
				if(commandMap==null||commandMap.isEmpty()){
					log.error(" the format path not found or error,don't parse to command temp");
					TaskAction.updateTaskStatus(taskID, TaskInfo.Status.FAIL);
					continue;
				}
				
				if("url".equalsIgnoreCase(type)){
					threadPools.execute(new DownLoad(commandMap,taskID,true,this));
				}else
					if("format".equalsIgnoreCase(type)){//不需要接受url，直接根据模版文件下载。
						threadPools.execute(new DownLoad(commandMap,taskID,false,this));
					}
			} catch (Exception e) {
				if(message!=null){
					rabbit.rejectMessage(message.getResponseTag(), publicQueue,false);
				}
				if(taskID!=null){
					synchronized (taskList) {
						this.taskList.remove(taskID);
					}
				}
				log.error(" download monitor is error :"+e.getMessage(),e);
			}
		}
	}
	
	
	
	public void receviceQueueInfo(KVBean kv){
		
				int index=kv.getKey().indexOf(":");
				
				if(index<1||kv.getValue()==null){
					log.error("not found taskID or the format path is not set!that is required. cancle this task");
					return;
				}
				
				String type=kv.getKey().substring(0,index);
				String taskID=kv.getKey().substring(index+1);
				String formatPath=kv.getValue();
				log.info("receive task from "+publicQueue+",the type is"+type+" taskID:"+taskID+" formatPath is:"+formatPath);
				
				synchronized (taskList) {
					if(this.taskList.contains(taskID)){
						log.warn(" the task was already exists !");
						return;
					}else{
						this.taskList.add(taskID);
					}
				}
				
				List<String> formats=readHdfsFile(formatPath);
				Map<String,List<Command>> commandMap=null;
				if(formats!=null)
					commandMap=CommandParse.parse(formats);
				
				if(commandMap==null||commandMap.isEmpty()){
					log.error(" the format path not found or error,don't parse to command temp");
					TaskAction.updateTaskStatus(taskID, TaskInfo.Status.FAIL);
					return;
				}
				
				if("url".equalsIgnoreCase(type)){
					threadPools.execute(new DownLoad(commandMap,taskID,true,this));
				}else
					if("format".equalsIgnoreCase(type)){//不需要接受url，直接根据模版文件下载。
						threadPools.execute(new DownLoad(commandMap,taskID,false,this));
					}
			
	}
	
	
	private void sleep(long sleepTime){
		log.debug(" sleep "+sleepTime);
		try {
			Thread.sleep(sleepTime);
		} catch (InterruptedException e) {}//等待并释放资源，2分钟检查一次
		log.debug(" sleep time out,continue receive message");
	}
	
	private  List<String> readHdfsFile(String formatPath) {
		return readHdfsFile(formatPath,3);
	}
	
	private  List<String> readHdfsFile(String formatPath,int repeat) {
			Path path=new Path(formatPath);
			try{
				if(!dfsFileSystem.exists(path)){
					log.error(" the format path is not exist!");
					return null;
				}
				FSDataInputStream fis=dfsFileSystem.open(path);
				LineNumberReader lnr=new LineNumberReader(new InputStreamReader(fis));
				List<String> formats=new ArrayList<String>();
				String line=null;
				while( (line=lnr.readLine())!=null){
					formats.add(line);
				}
				return formats;
			}catch (Exception e) {
				log.error(" read hdfs file is fail,don't init format",e);
				log.info(" try again after 2 minute",e);
				if(repeat>0){
					sleep(1000*60*2);
					initHdfsFileSystem(false);
					readHdfsFile(formatPath, --repeat);
				}
				return null;
			}
	}

	public static void main(String[] args) throws Exception {
		SpiderContext.initSpiderContentByDefaultFile();
		SpiderConfig config=SpiderContext.getConfig();
		DownloadManager dm=null;
		dm=new DownloadManager(config);
		dm.receviceQueueInfo();
		log.info(" download is start ");
	}

	@Override
	public synchronized void reportStatus(String taskID, DownloadStatus.Status status) {
		if(status==DownloadStatus.Status.STOP){
			log.info(" the download task "+taskID+" is finish:");
			--usedThread;
			this.taskList.remove(taskID);
		}
		else if(status==DownloadStatus.Status.START){
			    log.info(" the download task "+taskID+" is start:");
				++usedThread;
				this.taskList.add(taskID);
			 }
	}
	
	/**
	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();//.addDefaultResource(DownloadManager.class.getResource("core-site.xml").getPath());
		FileSystem file = FileSystem.get(conf);
		FileStatus[] fses=file.listStatus(new Path("/spider/formats/agoda.image"));
		List<String> formats=new ArrayList<String>();
		for(FileStatus fs:fses){
			System.out.println(fs.getPath().toString());
			FSDataInputStream fis=file.open(fs.getPath());
			LineNumberReader lnr=new LineNumberReader(new InputStreamReader(fis));
			StringBuffer sb=new StringBuffer();
			String line=null;
			while( (line=lnr.readLine())!=null){
				sb.append(line).append("\r\n");
			}
			if(sb.length()>0) formats.add(sb.toString());
		}
	}
	**/
}


