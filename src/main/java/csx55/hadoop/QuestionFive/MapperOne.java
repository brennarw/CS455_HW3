package csx55.hadoop.QuestionFive;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//uses both data sets
public class MapperOne extends Mapper<Object, Text, Text, IntWritable> {

    private IntWritable duration = new IntWritable();
    private Text songID = new Text();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        //key = songID
        //value = duration
        
        String[] attributes = value.toString().split("\\|");

        songID.set(attributes[0]); //TODO: change this to the correct index
        duration.set(Integer.parseInt(attributes[0])); //TODO: change this to the correct index

        context.write(songID, duration);


        

    }
}

