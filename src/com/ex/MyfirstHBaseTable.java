package com.ex;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.hadoop.hive.shims.HadoopShims.HCatHadoopShims;
public class MyfirstHBaseTable {
	private static String driverName = "org.apache.hive.jdbc.HiveDriver";
	/**
	 * @param args
	 * @throws ClassNotFoundException 
	 * @throws SQLException 
	 */
	public static void main(String[] args) throws ClassNotFoundException, SQLException {// Register driver and create driver instance
	      Class.forName(driverName);
	      // get connection
	      Connection con = DriverManager.getConnection("jdbc:hive2://quickstart.cloudera:10000/default", "", "");
	      
	      // create statement
	      Statement stmt = con.createStatement();
	      
	      // execute statement
	      stmt.execute("CREATE TABLE IF NOT EXISTS "+" employee ( eid int, name String, "+" salary String, destignation String)"+" COMMENT 'Employee details'"+" ROW FORMAT DELIMITED"+" FIELDS TERMINATED BY '\t'"+" LINES TERMINATED BY '\n'"+" STORED AS TEXTFILE");
	         
	      System.out.println("Table employee created.");
	      con.close();
	      
	}
		
	
	/*public void connectHive(){
		
		HBaseConfiguration hconfig = new HBaseConfiguration(new Configuration());
	    HTableDescriptor htable = new HTableDescriptor("User"); 
	    htable.addFamily( new HColumnDescriptor("Id"));
	    htable.addFamily( new HColumnDescriptor("Name"));
	    System.out.println( "Connecting..." );
	    HBaseAdmin hbase_admin = new HBaseAdmin( hconfig );
	    System.out.println( "Creating Table..." );
	    hbase_admin.createTable( htable );
	    System.out.println("Done!");
	}*/

}
