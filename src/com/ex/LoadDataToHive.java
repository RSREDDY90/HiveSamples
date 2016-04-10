package com.ex;
import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
public class LoadDataToHive {
	
	private static String driverName = "org.apache.hive.jdbc.HiveDriver";

public static void main(String[] args) throws SQLException, ClassNotFoundException {

   // Register driver and create driver instance
   Class.forName(driverName);
   
   // get connection
   Connection con = DriverManager.getConnection("jdbc:hive2://quickstart.cloudera:10000/default", "", "");
   
   // create statement
   Statement stmt = con.createStatement();
   
   // execute statement
   //stmt.execute("LOAD DATA LOCAL INPATH '/home/cloudera/workspace/HiveSample/config/sample2.txt'" + "OVERWRITE INTO TABLE employee");
   
   //stmt.execute("LOAD DATA LOCAL INPATH '/home/cloudera/workspace/HiveSample/config/bangAddress.txt'" + "OVERWRITE INTO TABLE customers");
   //stmt.execute("LOAD DATA LOCAL INPATH '/home/cloudera/workspace/HiveSample/config/apAddress.txt'" + "INTO TABLE customers");
   
   //test for below
   stmt.execute("LOAD DATA LOCAL INPATH '/home/cloudera/workspace/HiveSample/config/bangAddress.txt'" + "INTO TABLE customers PARTITION(country='india', state='KA')");
   //LOAD DATA LOCAL INPATH '/home/cloudera/workspace/HiveSample/config/bangAddress.txt' INTO TABLE customers PARTITION(country='india', state='KA', type);
   System.out.println("Load Data into employee successful");
   
   con.close();
}}
