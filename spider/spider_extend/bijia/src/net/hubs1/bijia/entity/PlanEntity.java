package net.hubs1.bijia.entity;

public class PlanEntity {
	
	public String sql;
	public int planId;
	public int ctripID;
	public int rid;
	public int planName;
	public int breakfastNum;
	public int broadBand;
	public int payType;
	public int ctripNumId;
	public int status;
	public int site;
	public int provider;
	private static PlanEntity instanceInsert = null;
	private static PlanEntity instanceUpdate = null;
	private static PlanEntity instanceSelect = null;
	private PlanEntity(){
	}
	public synchronized static PlanEntity getInsertInstance(){
		if(instanceInsert == null){
			instanceInsert = new PlanEntity();
			instanceInsert.sql= "insert into ctripPlan (planId,ctripID,rid,planName," +
					"breakfastNum,broadBand,payType,ctripNumId,status)values(?,?,?,?,?,?,?,?,?)";
			instanceInsert.planId=1;
			instanceInsert.ctripID=2;
			instanceInsert.rid=3;
			instanceInsert.planName=4;
			instanceInsert.breakfastNum=5;
			instanceInsert.broadBand=6;
			instanceInsert.payType=7;
			instanceInsert.ctripNumId=8;
			instanceInsert.status=9;
		}
		return instanceInsert;
	}
	
	public synchronized static PlanEntity getUpdateInstance(){
		if(instanceUpdate == null){
			instanceUpdate = new PlanEntity();
			instanceUpdate.sql= "update ctripPlan set breakfastNum=?," +
					"broadBand=?,payType=?,ctripNumId=?,status=? where planId=?";
			instanceUpdate.breakfastNum=1;
			instanceUpdate.broadBand=2;
			instanceUpdate.payType=3;
			instanceUpdate.ctripNumId=4;
			instanceUpdate.status=5;
			instanceUpdate.planId=6;
		}
		return instanceUpdate;
	}
	
	public synchronized static PlanEntity getSelectInstance(){
		if(instanceSelect == null){
			instanceSelect = new PlanEntity();
			instanceSelect.sql= "select * from  ctripPlan where planId=?";
			instanceSelect.planId=1;
		}
		return instanceSelect;
	}
	
	public synchronized static PlanEntity getQunarUpdateInstance(){
		if(instanceUpdate == null){
			instanceUpdate = new PlanEntity();
			instanceUpdate.sql= "update ctripPlan set payType=?,status=? where planId=?";

			instanceUpdate.payType=1;
			instanceUpdate.status=2;
			instanceUpdate.planId=3;
		}
		return instanceUpdate;
	}
	
	public synchronized static PlanEntity getQunarInsertInstance(){
		if(instanceInsert == null){
			instanceInsert = new PlanEntity();
			instanceInsert.sql= "insert into ctripPlan (planId,ctripID,rid,planName," +
					"payType,status,site,provider)values(?,?,?,?,?,?,?,?)";
			instanceInsert.planId=1;
			instanceInsert.ctripID=2;
			instanceInsert.rid=3;
			instanceInsert.planName=4;
			instanceInsert.payType=5;
			instanceInsert.status=6;
			instanceInsert.site=7;
			instanceInsert.provider=8;
		}
		return instanceInsert;
	}
}
