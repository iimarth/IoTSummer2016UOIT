package iotprojectspark;

//importing necessary dependencies
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Random;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.codehaus.jettison.json.JSONObject;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;

import de.farberg.spark.examples.streaming.ServerSocketSource;
import scala.Tuple2;
import scala.Tuple4;

public class SparkIoT {
	//intitialize member variables: usernumber, roomnumber, bodytemperature, roomtemperature
	private int userNum;
	private int roomNum;
	private double bodyTemp;
	private double roomTemp;
	
	//overloaded constructor for data generation
	public SparkIoT(int u, int r, double rt) {
		userNum = u;
		roomNum = r;
		roomTemp = rt;
		Random rn = new Random();
		bodyTemp = 30 + 15 * rn.nextDouble();
	}
	//getters and setters
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
		
				// if the bodytemperature is below the roomtemperature, the bodytemperature will rise slightly
				if (bodyTemp < roomTemp) {
					bodyTemp += Math.random(); // to get a double between 0 and 1;
				}
				// if it is above, the bodytemperature will drop
				else if (bodyTemp > roomTemp) {
					bodyTemp -= Math.random();
				}// Code for sliding body temperature to room temperature goes here
	}
	
	public void adjustRoomTemp(double avgBodytemp){
		if(avgBodytemp < 36){
			roomTemp += Math.random();//to get a double between 0 and 1;
		}
		else if(avgBodytemp > 37.5){
			roomTemp -= Math.random();//to get a double between 0 and 1;
		}
		//else: everything is fine, roomtemperature won´t be changed then
	}

	public String toString() {
		return this.userNum + "," + this.roomNum + "," + this.bodyTemp + "," + this.roomTemp;
	}

	public static void main(String[] args) {
		//create list of user objects and fill with data
		ArrayList<SparkIoT> userData = new ArrayList<SparkIoT>();
		for (int count = 100; count < 200; count++) {
			if (count >= 100 && count < 120) {
				userData.add(new SparkIoT(count, 1, 38));
			}
			if (count >= 120 && count < 140) {
				userData.add(new SparkIoT(count, 2, 35));
			}
			if (count >= 140 && count < 160) {
				userData.add(new SparkIoT(count, 3, 40));
			}
			if (count >= 160 && count < 180) {
				userData.add(new SparkIoT(count, 4, 33));
			}
			if (count >= 180 && count < 200) {
				userData.add(new SparkIoT(count, 5, 36));
			}
		}

		//cycle object info to listening socket
		Iterator<SparkIoT> cycleIterator = Iterators.cycle(userData);

		ServerSocketSource<String> dataSource = new ServerSocketSource<>(() -> {
			SparkIoT next = cycleIterator.next();
			//adjust body temperature since we are cycling already
			next.adjustBodyTemp();
			
			if (next == userData.get(0)) {
				try {
					Thread.sleep(5000); // sleep to reduce interrupts & race condition
				} catch (Exception e) {
					e.printStackTrace();
				}
			}

			System.out.println("Sending: " + next.toString());

			return next.toString();
		});

		final String host = "localhost";

		// Create a server socket data source that sends string values every
		
		// Create the context with a 1 second batch size
		SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("JavaNetworkWordCount");
		JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(10));
		// Create a JavaReceiverInputDStream on target ip:port and count the
		// words in input stream of \n delimited text
		JavaReceiverInputDStream<String> lines = ssc.socketTextStream(host, dataSource.getLocalPort(),
				StorageLevels.MEMORY_AND_DISK_SER);

		JavaDStream<String> temperatureLines = lines.flatMap(data ->Lists.newArrayList(data.split(",")));
		
		// "personid, room, temp, roomTemp"
		JavaDStream<Tuple4<Integer, Integer, Double, Double>> mappedValues = lines.map(line -> {
			String[] split = line.split(",");
			return new Tuple4<>(Integer.parseInt(split[0]),
			Integer.parseInt(split[1]), Double.parseDouble(split[2]),
			Double.parseDouble(split[3]));
			});

		JavaPairDStream<Integer, Double> pair =
			mappedValues.mapToPair(entry -> new Tuple2<>(entry._2(), entry._3()));
			JavaPairDStream<Integer, Double> reduceByKey =
			pair.reduceByKey((a,b) -> b);
		//average would have been calculated here
						
		mappedValues.print();
		ssc.start();

		ssc.awaitTermination();
		ssc.close();

		dataSource.stop();
	}

}
