package csx55.hadoop.QuestionFive;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//uses both data sets
//have the mapper use the key=one so that all songs are sent to the same reducer
public class MapperOne extends Mapper<Object, Text, IntWritable, Text> {

    private final static IntWritable one = new IntWritable(1); //key
    //private Text songID = new Text(); //value
    //private IntWritable duration = new IntWritable(); //value

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        //key = one
        //value = songID, duration
        
        String[] attributes = value.toString().split("\\|");

        String songDuration = attributes[0] + "|" + attributes[4]; //songID|duration 

        context.write(one, new Text(songDuration));


        

    }
}

