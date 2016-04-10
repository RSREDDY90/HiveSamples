package com.ex;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class HiveSelectQuery {

	private static String driverName = "org.apache.hive.jdbc.HiveDriver";
	/**
	 * @param args
	 * @throws ClassNotFoundException 
	 * @throws SQLException 
	 */
	public static void main(String[] args) throws ClassNotFoundException, SQLException {
	




		   // Register driver and create driver instance
		   Class.forName(driverName);
		   
		   // get connection
		   Connection con = DriverManager.getConnection("jdbc:hive2://quickstart.cloudera:10000/default", "", "");
		   
		   // create statement
		   Statement stmt = con.createStatement();
		   
		   
		   //"output/sms"+File.separator+"part-00000"
		   // execute statement
		   ResultSet rs = stmt.executeQuery("select * from pizz_service LIMIT 5");
		   while(rs.next()){
			   System.out.println("name is:"+rs.getString(1)+" Status is:"+rs.getString(2));
			   
		   }
		   System.out.println("Load Data into employee successful");
		   
		   con.close();

	
		
		
	}
	
/*	Configuration config = new Configuration();
    //get the configuration parameters and assigns a job name
    JobConf job = new JobConf(config, SmsDriver.class);
    System.out.println("Working directory is:"+job.getWorkingDirectory());
    job.setJobName("SMS Reports");
    //Configuration.set("mapreduce.output.key.field.separator", ",");
    config
    job.set("mapred.textoutputformat.separator", ",");
    //conf.getConfiguration().set("mapreduce.output.basename", "text");
    //setting key value types for mapper and reducer outputs
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    //specifying the custom reducer class
    job.setReducerClass(SMSReducer.class);

    //Specifying the input directories(@ runtime) and Mappers independently for inputs from multiple sources
    MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, UserFileMapper.class);
    MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, DelivaryDetailsMapper.class);
   
    //Specifying the output directory @ runtime
    FileOutputFormat.setOutputPath(job, new Path(args[2]));
    
    

    JobClient.runJob(job);*/

}
