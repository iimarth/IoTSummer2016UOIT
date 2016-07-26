package Project.TempAlgorithm;

import java.util.ArrayList;

public class Room {

	private static int roomIdCounter = 1;
	
	private int mRoomId;
	private ArrayList <Person> mPersons; 
	private double mTemperature; 
	
	public Room(ArrayList <Person> aPersons){
		
		mPersons = aPersons;
		mRoomId = roomIdCounter++;
		mTemperature = (Math.random()*10)+15; 
		
	}
	
	public double getAvgTemp(){
		
		double avgTemp = 0; 
		for(int i = 0; i < this.mPersons.size(); i++){
			
			avgTemp += this.mPersons.get(i).getmTemperature();
			
		}
		avgTemp = avgTemp/this.mPersons.size();
		return avgTemp; 
	}
	
	private boolean inBetween(double var, double max, double min){
		
	return min <= var && var <= max;	
		
	}
	
	public void compareTemp(){
		
		double avgTemp = getAvgTemp();
		
		if(inBetween(avgTemp, 50.0, 42.0)){
			setmTemperature(getmTemperature()-2.5);
		}else if(inBetween(avgTemp, 42.0, 40.0)){
			setmTemperature(getmTemperature()-2.0);
		}else if(inBetween(avgTemp, 40.0, 38.5)){
			setmTemperature(getmTemperature()-1.0);
		}else if(inBetween(avgTemp, 38.5, 36.0)){
			setmTemperature(getmTemperature()-0.5);
		}else if(inBetween(avgTemp, 36.0, 33.0)){
			setmTemperature(getmTemperature()-0.2);
		}else if(inBetween(avgTemp, 33.0, 28.0)){
			//
		}else if(inBetween(avgTemp, 28.0, 26.0)){
			setmTemperature(getmTemperature()+0.2);
		}else if(inBetween(avgTemp, 26.0, 24.5)){
			setmTemperature(getmTemperature()+0.5);
		}else if(inBetween(avgTemp, 22.0, 15.0)){
			setmTemperature(getmTemperature()+1.0);
		}
				
			
		
		
		
	}

	public ArrayList<Person> getmPersons() {
		return mPersons;
	}

	public boolean addPersons(ArrayList<Person> aPersons) {
		
		if(aPersons.size()==0)
			return false;
		
		for(int i = 0; i < aPersons.size(); i++){
			mPersons.add(aPersons.get(i));
		}
		return true; 		
		
	}
	
	public boolean removePersons(int...params){
		for(int i:params){
			
			if(i>=this.mPersons.size())
				return false;
			this.mPersons.remove(i);
			
		}
		
		return true;
	}

	public double getmTemperature() {
		return mTemperature;
	}

	public void setmTemperature(double mTemperature) {
		this.mTemperature = mTemperature;
	}

	public int getmRoomId() {
		return mRoomId;
	}
	
	
	
}
