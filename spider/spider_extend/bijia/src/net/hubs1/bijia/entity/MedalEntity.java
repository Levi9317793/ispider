package net.hubs1.bijia.entity;

public class MedalEntity {
	
	public String sql;
	public int ctripId;
	public int goldLevel;
	private static MedalEntity instanceInsert = null;
	private static MedalEntity instanceUpdate = null;
	private MedalEntity(){
	}
	public synchronized static MedalEntity getInsertInstance(){
		if(instanceInsert == null){
			instanceInsert = new MedalEntity();
			instanceInsert.sql = "insert into ctripGold (ctripId,goldLevel)values(?,?)";
			instanceInsert.ctripId=1;
			instanceInsert.goldLevel=2;
		}
		return instanceInsert;
	}
	
	public synchronized static MedalEntity getUpdateInstance(){
		if(instanceUpdate == null){
			instanceUpdate = new MedalEntity();
			instanceUpdate.sql= "update ctripGold set goldLevel=? where ctripId=?";
			instanceUpdate.goldLevel=1;
			instanceUpdate.ctripId=2;
		}
		return instanceUpdate;
	}
	
}
