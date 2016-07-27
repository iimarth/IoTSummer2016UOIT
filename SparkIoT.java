package iotprojectspark;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Random;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;

import de.farberg.spark.examples.streaming.ServerSocketSource;
import scala.Tuple2;
import scala.Tuple4;

import java.util.ArrayList;
import java.util.Random;

public class SparkIoT {

	private int userNum;
	private int roomNum;
	private double bodyTemp;
	private double roomTemp;

	public SparkIoT(int u, int r, double rt) {
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
		
				// if the bodytemperature is below the roomtemperature, the
				// bodytemperature will rise slightly
				if (bodyTemp < roomTemp) {
					bodyTemp += Math.random(); // to get a double between 0 and 1;
				}
				// if it is above, the bodytemperature will drop
				else if (bodyTemp > roomTemp) {
					bodyTemp -= Math.random();
				}// Code for sliding body temp to room temp goes here
	}
	
	public void adjustRoomTemp(double avgBodytemp){
		if(avgBodytemp < 36){
			roomTemp += Math.random();//to get a double between 0 and 1;
		}
		else if(avgBodytemp > 37.5){
			roomTemp -= Math.random();//to get a double between 0 and 1;
		}
		//else: everything is fine, roomtemp won´t be changed then
	}

	public String toString() {
		return this.userNum + "," + this.roomNum + "," + this.bodyTemp + "," + this.roomTemp;
	}

	public static void main(String[] args) {
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
		// for(int i = 0; i<100; i++) {
		/*
		 * for(int i=0; i<10; i++) { try { Thread.sleep(5000); }
		 * catch(InterruptedException ie) {}
		 * 
		 * for (UserInfo p : userData) { System.out.println(p); } }
		 */

		Iterator<SparkIoT> cycleIterator = Iterators.cycle(userData);

		ServerSocketSource<String> dataSource = new ServerSocketSource<>(() -> {
			SparkIoT next = cycleIterator.next();

			if (next == userData.get(0)) {
				try {
					Thread.sleep(5000);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

			System.out.println("Sending: " + next.toString());

			return next.toString();
		});

		final String host = "localhost";

		// Create a server socket data source that sends string values every
		// 100mss

		// Create the context with a 1 second batch size
		SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("JavaNetworkWordCount");
		JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(10));
		//JavaSparkContext sc = new JavaSparkContext(sparkConf);
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
						

		mappedValues.print();
		/*JavaPairDStream<String, String> temperature = lines.mapToPair(line -> {
           String[] temp = line.split(",");
          
            Tuple2 t= new Tuple2<>(Integer.parseInt(temp[1]), Double.parseDouble(temp[2]));
			return t;
							
            });*/
		
		//JavaRDD<SparkIoT> sparkData = sc.parallelize(userData);
		//JavaRDD<String> Data = sparkData.flatMap(sparkData -> Lists.newArrayList(sparkData.split(",")));
		//JavaPairRDD<String, Integer> wordMap = Data.mapToPair(word -> new Tuple2<>(word, 1));
		/*val[].class = SparkIoT.split(",");
		new Tuple2 <Integer, Double> (val[1], val[2]);
		*/
		//JavaPairRDD<String, Integer> reduceByKey = wordMap.reduceByKey((a, b) -> b);
		
		//JavaDStream<String> words = lines.flatMap(x -> Lists.newArrayList(x.split(",")));

		//JavaPairDStream<String, Integer> wordCounts = words.mapToPair(word -> new Tuple2<String, Integer>(word, 1))
		//		.reduceByKey((i1, i2) -> i1 + i2);

		//wordCounts.print();
		ssc.start();

		ssc.awaitTermination();
		ssc.close();

		dataSource.stop();
	}

}
