package net.hubs1.bijia.entity;

public class QunarHotelEntity {
	
	public String sql;
	public int pdcId;
	public int province;
	public int city;
	public int name;
	public int star;
	private static QunarHotelEntity instanceInsert = null;
	private static QunarHotelEntity instanceSelect = null;
	private QunarHotelEntity(){
	}
	
	public static QunarHotelEntity getSelectInstance(){
		if(instanceSelect == null){
			instanceSelect = new QunarHotelEntity();
			instanceSelect.sql= "select id from  qunarHotel where pdcId=?";
			instanceSelect.pdcId=1;
		}
		return instanceSelect;
	}
	
	public static QunarHotelEntity getInsertInstance(){
		if(instanceInsert == null){
			instanceInsert = new QunarHotelEntity();
			instanceInsert.sql=  "insert into qunarHotel (pdcId,province,city,name,star)" +
					"values(?,?,?,?,?)";
			instanceInsert.pdcId=1;
			instanceInsert.province=2;
			instanceInsert.city=3;
			instanceInsert.name=4;
			instanceInsert.star=5;
		}
		return instanceInsert;
	}
}
	