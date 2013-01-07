package net.hubs1.spider.web;



import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import net.hubs1.spider.core.CommandParse;
import net.hubs1.spider.core.DBClient;
import net.hubs1.spider.core.Task;
import net.hubs1.spider.core.action.TaskAction;
import net.hubs1.spider.core.action.TaskInfo;
import net.hubs1.spider.core.config.SpiderContext;
import net.hubs1.spider.scheduler.QuartzManager;
import net.hubs1.spider.scheduler.SpiderScheduler;
import net.hubs1.spider.store.DBFactory;
import net.hubs1.spider.store.KVBean;
import net.hubs1.spider.store.RecordsBean;
import net.hubs1.spider.util.SpiderUtil;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;


public class SpiderServlet extends HttpServlet{
	private static final long serialVersionUID = -907186112337568934L;
	private static FileSystem fsFileSystem=null;
	private static Logger log=Logger.getLogger(SpiderServlet.class);
	
	
	@Override
	public void init() throws ServletException {
		super.init();
		Configuration conf = new Configuration();
		try {
			fsFileSystem = FileSystem.get(conf);
		} catch (IOException e) {
			log.error(" init FS FileSystem is fail!",e);
		}
	}
	
	@Override
	public void doGet(HttpServletRequest req,HttpServletResponse resp) throws javax.servlet.ServletException, java.io.IOException{
		doPost(req,resp);
	}
	
	@Override
	public void doPost(HttpServletRequest req,HttpServletResponse resp) throws javax.servlet.ServletException, java.io.IOException{
		String method=req.getParameter("method");
		if("submitFormat".equals(method)){
			String format=req.getParameter("format");
			String path=req.getParameter("basepath");
			String type=req.getParameter("extp");
			String fileName=req.getParameter("fileName");
			
			String isUpdate=req.getParameter("update");
			
			String message=CommandParse.testFormat(format);
			if(message==null){//表示测试成功
				message=submitFormat(path+type, fileName, format,"true".equals(isUpdate));
			}
			
			resp.setContentLength(message.getBytes().length);
			resp.setContentType("txt/html;charset=utf-8");
			resp.setStatus(200);
			OutputStream out=resp.getOutputStream();
			out.write(message.getBytes());
			out.flush();
			out.close();
			return;
		}
		
		if("listFile".equals(method)){
			String path=req.getParameter("basepath");
			JSONObject obj=listFile(path);
			resp.setContentType("application/javascript;charset=utf-8");
			resp.setStatus(200);
			if(obj!=null){
				byte[] content=obj.toString().getBytes();
				resp.setContentLength(content.length);
				OutputStream out=resp.getOutputStream();
				out.write(content);
				out.flush();
				out.close();
			}else{
				resp.setContentLength(0);
			}
			return ;
		}
		
		if("testFormat".equals(method)){
			resp.setContentType("plain/text;charset=utf-8");
			resp.setStatus(200);
			String filePath=req.getParameter("formatFilePath");
			String url=req.getParameter("test_url");
			File file=testFormat(filePath, url);
			if(file!=null&&file.exists()&&file.isFile()){
				OutputStream out=resp.getOutputStream();
				FileInputStream is=new FileInputStream(file);
				int r=-1;
				while((r=is.read())!=-1){
					out.write(r);
				}
				out.flush();
				out.close();
			}
			return ;
		}
		
		if("createTask".equals(method)){
			this.createTask(req, resp);
			return;
		}
		
		if("taskList".equals(method)){
			this.taskList(req, resp);
		}
		
		if("runTask".equals(method)){
			runTask(req,resp);
		}
		
		if("deleteTask".equals(method)){
			deleteTask(req,resp);
		}
		
		if("fsOpt".equals(method)){
			fsOpt(req,resp);
		}
	}
	
	private void fsOpt(HttpServletRequest req,HttpServletResponse resp) throws IOException{
		String type=req.getParameter("type");
		String message="";
		String path=req.getParameter("path");
		Path nf=new Path(path);
		if("view".equalsIgnoreCase(type)){
			message=readTemp(path, true);
		}
		if("edit".equalsIgnoreCase(type)){
			message=readTemp(path,false);
		}
		if("save".equalsIgnoreCase(type)){
			String format=req.getParameter("format");
			if(fsFileSystem.exists(nf)){
				fsFileSystem.delete(nf,false);
				fsFileSystem.createNewFile(nf);
				FSDataOutputStream fsOut=fsFileSystem.append(nf);
				fsOut.write(format.getBytes());
				fsOut.flush();
				fsOut.close();
				message="保存成功";
			}else{
				message="文件不存在";
			}
		}
		if("move".equalsIgnoreCase(type)){
			String tpath=req.getParameter("target");
			if(fsFileSystem.exists(nf)){
				Path tp=new Path(tpath);
				message=fsFileSystem.rename(nf, tp)?"文件移动成功":"文件移动失败";
			}else{
				message="指定文件不存在";
			}
		}
		if("copy".equalsIgnoreCase(type)){
			String tpath=req.getParameter("target");
			Path tp=new Path(tpath);
			message=FileUtil.copy(fsFileSystem, nf, fsFileSystem, tp, false,fsFileSystem.getConf())?"文件复制成功":"文件复制失败";
		}
		
		if("delete".equalsIgnoreCase(type)){
			if(fsFileSystem.exists(nf)){
				message=fsFileSystem.delete(nf, false)?"删除成功":"删除失败";
			}else{
				message="指定文件/路径不存在";
			}
		}
		
		if("mkdir".equalsIgnoreCase(type)){
			String tpath=req.getParameter("target");
			
			if(fsFileSystem.isFile(nf)){
				path=path.substring(0,path.lastIndexOf("/"));
			}
			
			Path tp=tpath.startsWith("/")?new Path(tpath):new Path(path+"/"+tpath);
			message=fsFileSystem.mkdirs(tp)?"目录建立成功":"目录建立失败";
		}
		outMessage(resp, message);
	}
	private String submitFormat(String path,String fileName,String format,boolean isUpdate) {
		Path fsPath=new Path(path);
		try{
			if(!fsFileSystem.exists(new Path(path))){
				fsFileSystem.mkdirs(fsPath);
			}
			
			if(format!=null&&format.length()>10){
				Path nf=new Path(path+"/"+fileName);
				
				if(fsFileSystem.exists(nf)){
					if(isUpdate) fsFileSystem.delete(nf,false);
				}
				if(!fsFileSystem.exists(nf)){
					fsFileSystem.createNewFile(nf);
					FSDataOutputStream fsOut=fsFileSystem.append(nf);
					fsOut.write(format.getBytes());
					fsOut.flush();
					fsOut.close();
					return "模版文件已保存成功";
				}else{
					return "文件已存在，请修改名称或者路径";
				}
			}
			return "文件保存失败";
		}catch (IOException e) {
			return "保存文件时发生IO错误，具体错误原因如下：\r\n"+SpiderUtil.exception2String(e);
		}
	}
	
	private File testFormat(String formatFile,String url) throws IOException{
		Path fsPath=new Path(formatFile);
		if(fsFileSystem.isFile(fsPath)){
			FSDataInputStream fis=fsFileSystem.open(fsPath);
			LineNumberReader lnr=new LineNumberReader(new InputStreamReader(fis));
			List<String> format=new ArrayList<String>();
			String line=null;
			while( (line=lnr.readLine())!=null){
				format.add(line);
			}
			
			String testf=SpiderContext.getConfig().getSaveFilePath()+"/"+"spider_out.db";
			File f=new File(testf);
			if(f.exists()){
				f.delete();
			}
			
			String mode=SpiderContext.getConfig().getOutMode();
			
			SpiderContext.getConfig().setOutMode("file");
			
			Task task=new Task(CommandParse.parse(format),null);
			task.test(url,null);
			
			SpiderContext.getConfig().setOutMode(mode);
			
			return new File(SpiderContext.getConfig().getSaveFilePath()+"/"+"spider_out.db");
		}
		return null;
	}
	
	private JSONObject listFile(String path) throws IOException{
		Path fsPath=new Path(path);
		
		JSONObject robj=new JSONObject();
		JSONArray list=new JSONArray();
		
		if(fsFileSystem.isFile(fsPath)){
			robj.put("type", 1);
			robj.put("name", fsPath.getName());
			
			String content=readTemp(path,true);
			
			robj.put("content", content);
		}else{
			FileStatus[] fses=fsFileSystem.listStatus(fsPath);
			if(fses!=null){
				for(FileStatus fs:fses){
					JSONObject obj=new JSONObject();
					obj.put("isDir", fs.isDir());
					obj.put("name", fs.getPath().getName());
					list.add(obj);
				}
				robj.put("type",0);
				robj.put("list", list);
			}
		}
		return robj;
	}
	
	
	private String readTemp(String filePath,boolean needFormat) throws IOException{
		Path fsPath=new Path(filePath);
		
		if(!fsFileSystem.exists(fsPath))
			return "文件不存在";
			
		if(!fsFileSystem.isFile(fsPath))
			return "所指定不是文件类型";
		
		FSDataInputStream fis=fsFileSystem.open(fsPath);
		LineNumberReader lnr=new LineNumberReader(new InputStreamReader(fis));
		StringBuilder sb=new StringBuilder();
		String line=null;
		int lineNum=1;
		while( (line=lnr.readLine())!=null){
			line=line.trim();
			
			if(needFormat){
				sb.append("<label style='color:#336666;'>").append(lineNum++).append(".</label>&nbsp;&nbsp;");
				line=line.replaceAll("([a-zA-Z]+:)", "<b>$1</b>");
				if(line.startsWith("#")) line="<span style='color:#999999;font-style:italic;'>"+line+"</span>";
				if( !(line.trim().startsWith("block_")||line.trim().startsWith("endblock")) ){
					sb.append("&nbsp;&nbsp;&nbsp;&nbsp;");
				}else{
					line="<span style='color:#990000;font-weight:bold;'>"+line+"</span>";
				}
				sb.append(line).append("</br>");
			}else{
				sb.append(line).append("\r\n");
			}
		}
		return sb.toString();
	}
	
	private void createTask(HttpServletRequest req,HttpServletResponse resp) throws IOException{
		String name=req.getParameter("taskname");
		String desc=req.getParameter("taskdesc");
		String type=req.getParameter("task_type");
		String file=req.getParameter("formatfile");
		String increment=req.getParameter("increment");
		String fixedtime=req.getParameter("fixedtime");
		String datarangeStart=req.getParameter("rangeStart");
		String datarangeEnd=req.getParameter("rangeEnd");
		String queuetag=req.getParameter("queuetag");
		String preTask=req.getParameter("pretask");
		String nextTask=req.getParameter("nexttask");

		String sd=req.getParameter("startDate");
		String ed=req.getParameter("endDate");
		
		String isRun=req.getParameter("isrun");//是否立即执行
		String ip=getClientIpAddr(req);
		
		RecordsBean rb=new RecordsBean("base",String.valueOf(System.currentTimeMillis()));
		KVBean kvname=new KVBean("name",name);
		KVBean kvdesc=new KVBean("desc",desc);
		KVBean kvtype=new KVBean("type",type);
		KVBean kvfile=new KVBean("file",file);
		KVBean kvincre=new KVBean("increment",increment);
		KVBean kvfixedtime=new KVBean("fixedtime",fixedtime);
		KVBean cstatus=new KVBean("cstatus",TaskInfo.Status.NEW.name());
		KVBean kvqueuetag=new KVBean("queue",queuetag);
		KVBean runnum=new KVBean("runnum","0");
		KVBean runnow=new KVBean("runNow",isRun);
		
		KVBean startDate=new KVBean("startDate",sd);
		KVBean endDate=new KVBean("endDate",ed);
		
		
		KVBean kvnextTask=new KVBean("nexttask",((nextTask!=null&&nextTask.trim().length()==0)||"--".equals(preTask))?null:nextTask);
		
		KVBean startRange=null;
		if(datarangeEnd!=null &&datarangeEnd.trim().length()>0 )
			startRange=new KVBean("startrange",datarangeStart);
		
		KVBean endRange=null;
		if( datarangeStart!=null && datarangeEnd.trim().length()>0)
			endRange=new KVBean("endrange",datarangeEnd);
		
		KVBean kvip=new KVBean("creatorIP",ip);
		
		KVBean[] kvs=new KVBean[]{kvname,kvdesc,kvtype,kvfile,kvincre,kvfixedtime,startRange,endRange,kvip,
				                  cstatus,kvqueuetag,runnum,kvnextTask,runnow,startDate,endDate};
		
		rb.setAttributes(kvs);
		
		/*检查设置的模版文件是否存在*/
		if(file==null||!fsFileSystem.exists(new Path(file))){
			outMessage(resp, "指定的模版文件不存在");
			return ;
		}
		
		DBClient db=DBFactory.getDBClient();
		
		boolean isOK=db.writeData("spider_task", rb);
		String message=isOK?"任务保存成功":"任务保存失败";
		
		if(preTask!=null&&preTask.trim().length()>0&&!"--".equals(preTask)){
			RecordsBean prb=new RecordsBean("base",preTask);
			prb.setAttributes(new KVBean[]{new KVBean("nexttask",rb.getRowkeyStr())});
			isOK=db.writeData("spider_task", prb);
			message=message+"\r\n"+(isOK?"前置任务保存成功":"前置任务保存失败");
		}
		
		if(isOK&&fixedtime!=null){
			SpiderScheduler.getInstance().addFixedTimeTask(rb.getRowkeyStr());
			message=message+"\r\n"+"已添加每日定时任务";
		}
		
		if("true".equals(isRun)&&isOK){
			message=message+","+TaskAction.runTask(rb.getRowkeyStr());
		}
		
		
		resp.setContentLength(message.getBytes().length);
		resp.setContentType("txt/html;charset=utf-8");
		resp.setStatus(200);
		OutputStream out=resp.getOutputStream();
		out.write(message.getBytes());
		out.flush();
		out.close();
	}
	
	private void runTask(HttpServletRequest req,HttpServletResponse resp) throws IOException{
		
		String rowkey=req.getParameter("rowkey");
		rowkey=rowkey.replace("row", "");
		
		String message=TaskAction.runTask(rowkey,false);//通过页面运行的，全部为全局运行。不使用增量
		
		resp.setContentLength(message.getBytes().length);
		resp.setContentType("txt/html;charset=utf-8");
		resp.setStatus(200);
		OutputStream out=resp.getOutputStream();
		out.write(message.getBytes());
		out.flush();
		out.close();
	}
	
	private void taskList(HttpServletRequest req,HttpServletResponse resp) throws IOException{
		String column=req.getParameter("taskname");
		
		DBClient db=DBFactory.getDBClient();
		List<RecordsBean> rs=db.scan("spider_task","base",null,null,column==null?new String[0]:new String[]{column});
		FlexGridJSONData fgd=new FlexGridJSONData();
		fgd.setPage(1);
		fgd.setTotal(rs==null?0:rs.size());
		if(rs!=null&&!rs.isEmpty()){
			for(RecordsBean rb:rs){
				Map<String,Object> map=new HashMap<String,Object>();
				for(KVBean kv:rb.getAttributes()){
					map.put(kv.getKey(), kv.getValue());
				}
				fgd.addRow(new String(rb.getRowkey()),map);
			}
		}
		String message=fgd.toExtString();
		
		resp.setContentLength(message.getBytes().length);
		resp.setContentType("txt/html;charset=utf-8");
		resp.setStatus(200);
		OutputStream out=resp.getOutputStream();
		out.write(message.getBytes());
		out.flush();
		out.close();
	}
	
	private void deleteTask(HttpServletRequest req, HttpServletResponse resp) throws IOException {
		String rowkey=req.getParameter("rowkey");
		rowkey=rowkey.replace("row", "");
		
		DBClient db=DBFactory.getDBClient();
		
		boolean isOK=db.delete("spider_task",rowkey);
		if(isOK){
			QuartzManager.getInstance().removeJob(rowkey);
		}
		
		String message=isOK?"指定任务已删除":"指定任务删除失败！";
		
		resp.setContentLength(message.getBytes().length);
		resp.setContentType("txt/html;charset=utf-8");
		resp.setStatus(200);
		OutputStream out=resp.getOutputStream();
		out.write(message.getBytes());
		out.flush();
		out.close();
		
	}
	
	private void outMessage(HttpServletResponse resp,String message) throws IOException{
		resp.setContentLength(message.getBytes().length);
		resp.setContentType("txt/html;charset=utf-8");
		resp.setStatus(200);
		OutputStream out=resp.getOutputStream();
		out.write(message.getBytes());
		out.flush();
		out.close();
	}
	
	
	
    private String getClientIpAddr(HttpServletRequest request) {
    	
//        String ip = request.getHeader("x-forwarded-for");
//        if (ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip)) {
//            ip = request.getHeader("Proxy-Client-IP");
//        }
//        if (ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip)) {
//            ip = request.getHeader("WL-Proxy-Client-IP");
//        }
//        if (ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip)) {
//            ip = request.getRemoteAddr();
//        }
        return request.getRemoteAddr();
    }
}
