package net.hubs1.spider.core.command;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.log4j.Logger;
import net.hubs1.spider.core.BaseCommand;

public class DateCommand  extends BaseCommand {
	private static Logger log=Logger.getLogger("DateCommand");
	
	public DateCommand(int lineNum, String commandName, String[] params,String[] vars) {
		super(lineNum, commandName, params,vars);
	}

	public void date(String[] params) {
		
		int days=Integer.parseInt(params[0]);
		boolean currentDate=params.length>2?true:false;
		Date date=null;
		
		if(!currentDate){
			Calendar c=Calendar.getInstance();
			date=c.getTime();
		}else{
			SimpleDateFormat sdf=new SimpleDateFormat(params[2]);
			try {
				date=sdf.parse((String)this.inputData);
			} catch (ParseException e) {
				log.error("don't parse date,you input date is["+this.inputData+"],format rule is ["+params[2]+"],at line num:"+this.lineNum,e);
			}
		}
		
		if(days!=0){
			Calendar c=Calendar.getInstance();
			c.setTime(date);
			c.set(Calendar.DAY_OF_MONTH,c.get(Calendar.DAY_OF_MONTH)+days);
			date=c.getTime();
		}
		
		
		SimpleDateFormat sdf=new SimpleDateFormat(params[1]);
		String dateF=sdf.format(date);
		
		this.putResult2Context(dateF);
	}
	
	public void dateFormat(String[] params) {
		String date=(String)this.inputData;
		String outFormat=null;
		String inFormat=null;
		if(params.length==3){
			date=params[0];
			outFormat=params[1];
			inFormat=params[2];
		}else{
			outFormat=params[0];
			inFormat=params[1];
		}
		SimpleDateFormat sdf=new SimpleDateFormat(inFormat);
		try {
			Date d=sdf.parse(date);
			sdf=new SimpleDateFormat(outFormat);
			String dateF=sdf.format(d);
			this.putResult2Context(dateF);
		} catch (ParseException e) {
			log.equals(e);
			this.putResult2Context(null);
		}

		
	}
}
