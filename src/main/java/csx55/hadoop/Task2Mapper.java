package csx55.hadoop;

import java.io.IOException;
import java.util.StringTokenizer;

import javax.naming.Context;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Task2Mapper extends Mapper<Object, Text, Text, DoubleWritable> {
    
    private final static DoubleWritable loudness = new DoubleWritable();
    private Text songID = new Text();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        String[] lines = value.toString().split("\n"); //each line represents one artist
        
        for(String line : lines){
            String[] attributes = line.split("|");
            songID.set(attributes[0]); //key
            loudness.set(Double.parseDouble(attributes[9])); //value
            context.write(songID, loudness); //this is what writes each <key, value> pair to the reducer
        }
    }
}
