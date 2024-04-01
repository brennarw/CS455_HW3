package csx55.hadoop;

import java.io.IOException;
import java.util.StringTokenizer;

import javax.naming.Context;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TaskOneMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text artist = new Text();

    //this mapper needs to find each unique artistID and write each instance found to the reducer
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        String[] lines = value.toString().split("\n"); //each line represents one artist
        
        for(String line : lines){
            String[] attributes = line.split("|");
            artist.set(attributes[2]); //artistID is the third attribute in the line
            context.write(artist, one); //this is what writes each <key, value> pair to the reducer
        }
    }
}