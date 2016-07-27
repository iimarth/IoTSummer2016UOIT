package iot;

import java.util.ArrayList;
import java.util.Random;


public class UserInfo {
	private int userNum;
	private int roomNum;
	private double bodyTemp;
	private double roomTemp;
	
	public UserInfo(int u, int r, double rt) {
		userNum = u;
		roomNum = r;
		roomTemp = rt;
		Random rn = new Random();
		bodyTemp = 30 + 15 * rn.nextDouble();
	}
	public int getUserNum() {
		return this.userNum;
	}
	public void setUserNum(int value) {
		this.userNum = value;
	}
	public int getRoomNum() {
		return this.roomNum;
	}
	public void setRoomNum(int value) {
		this.roomNum = value;
	}
	public double getBodyTemp() {
		return this.bodyTemp;
	}
	public void setBodyTemp(double value) {
		this.bodyTemp = value;
	}
	public double getRoomTemp() {
		return this.roomTemp;
	}
	public void setRoomTemp(double value) {
		this.roomTemp = value;
	}
	public void adjustBodyTemp() {
		//Code for sliding body temp to room temp goes here
	}
	public String toString() {
		return this.userNum + "," + this.roomNum + "," + this.bodyTemp + "," + this.roomTemp;
	}


public static void main(String[] args) {
	ArrayList<UserInfo> userData = new ArrayList<UserInfo>();
	for(int count = 100; count<200; count++) {
		if(count>=100 && count<120) {
			userData.add(new UserInfo(count, 1, 38));
		}
		if(count>=120 && count<140) {
			userData.add(new UserInfo(count, 2, 35));
		}
		if(count>=140 && count<160) {
			userData.add(new UserInfo(count, 3, 40));
		}
		if(count>=160 && count<180) {
			userData.add(new UserInfo(count, 4, 33));
		}
		if(count>=180 && count<200) {
			userData.add(new UserInfo(count, 5, 36));
		}
	}
	//for(int i = 0; i<100; i++) {
	for(int i=0; i<10; i++) {
		try {
			Thread.sleep(5000);
		} catch(InterruptedException ie) {}
	
		for (UserInfo p : userData) {
			System.out.println(p);
		}
	}
}


}
