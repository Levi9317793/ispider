package net.hubs1.bijia.entity;

public class RateEntity {
	
	public String sql;
	public int pId;
	public int date;
	public int svcRate;
	public int breakfast;
	public int status;
	public int currency;
	public int site;
	public int provider;
	private static RateEntity instanceInsert = null;
	private static RateEntity instanceUpdate = null;
	private RateEntity(){
	}
	public synchronized static RateEntity getInsertInstance(){
		if(instanceInsert == null){
			instanceInsert = new RateEntity();
			instanceInsert.sql=  "insert into ctripRate (pId,date,svcRate,breakfast,status," +
					"currency) values(?,?,?,?,?,?)";
			instanceInsert.pId=1;
			instanceInsert.date=2;
			instanceInsert.svcRate=3;
			instanceInsert.breakfast=4;
			instanceInsert.status=5;
			instanceInsert.currency=6;
		}
		return instanceInsert;
	}
	
	public synchronized static RateEntity getUpdateInstance(){
		if(instanceUpdate == null){
			instanceUpdate = new RateEntity();
			instanceUpdate.sql= "update ctripRate set svcRate=?,breakfast=?,status=?," +
					"currency=? where pId=? and date=?";
			instanceUpdate.svcRate=1;
			instanceUpdate.breakfast=2;
			instanceUpdate.status=3;
			instanceUpdate.currency=4;
			instanceUpdate.pId=5;
			instanceUpdate.date=6;
		}
		return instanceUpdate;
	}
	
	public synchronized static RateEntity getQunarInsertInstance(){
		if(instanceInsert == null){
			instanceInsert = new RateEntity();
			instanceInsert.sql=  "insert into ctripRate (pId,date,svcRate,status," +
					"site,provider) values(?,?,?,?,?,?)";
			instanceInsert.pId=1;
			instanceInsert.date=2;
			instanceInsert.svcRate=3;
			instanceInsert.status=4;
			instanceInsert.site=5;
			instanceInsert.provider=6;
		}
		return instanceInsert;
	}
	
	
	public synchronized static RateEntity getQunarUpdateInstance(){
		if(instanceUpdate == null){
			instanceUpdate = new RateEntity();
			instanceUpdate.sql= "update ctripRate set svcRate=?,status=? where pId=? and date=?";
			instanceUpdate.svcRate=1;
			instanceUpdate.status=2;
			instanceUpdate.pId=3;
			instanceUpdate.date=4;
		}
		return instanceUpdate;
	}
}
