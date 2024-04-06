package csx55.hadoop.QuestionSix;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//uses both data sets
public class MapperOne extends Mapper<Object, Text, IntWritable, Text> {

    private final static IntWritable one = new IntWritable(1);

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        String[] attributes = value.toString().split("\\|");

        //System.out.println("danceability: " + attributes[3] + " | energy: " + attributes[6]);

        String reducerVal = attributes[0] + "|" + attributes[3] + "|" + attributes[6]; //songID|danceability|energy

        context.write(one, new Text(reducerVal));
        

    }
}

