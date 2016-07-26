package Project.TempAlgorithm;

public class Person {

	private static int idCounter = 1;
	private int mPid; 
	private double mTemperature; 

	public Person(double aTemperature){
		
		mTemperature = aTemperature;	
		mPid = idCounter++;
		
	}

	public double getmTemperature() {
		return mTemperature;
	}

	public void setmTemperature(double mTemperature) {
		this.mTemperature = mTemperature;
	}

	public int getmPid() {
		return mPid;
	}
	
	
	
}
