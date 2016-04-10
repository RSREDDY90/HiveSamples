package co.mapreduce.hive.example;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Mapper.Context;
public class HiveMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable>{
	
	private final static IntWritable one = new IntWritable(1);
	@Override
	public void map(LongWritable inputKey, Text inputValues,OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException {

		StringTokenizer stringTokenizer = new StringTokenizer(inputValues.toString());
		
		while(stringTokenizer.hasMoreTokens()){
			output.collect(new Text(stringTokenizer.nextToken()), one);
		}
		
	}

	public void setup(Context context) throws IllegalArgumentException, IOException {
    }
	

}
