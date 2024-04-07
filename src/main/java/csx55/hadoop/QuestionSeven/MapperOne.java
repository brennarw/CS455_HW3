package csx55.hadoop.QuestionSeven;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperOne extends Mapper<Object, Text, Text, Text> {

    private final static IntWritable one = new IntWritable(1); //key

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        //key: start-time | pitch | timbre | max-loudness | max-loudness-time | start-loudness
        //value: float value of each variable

        String[] attributes = value.toString().split("\\|");
        
        context.write(new Text("start-time"), new Text(attributes[17]));
        context.write(new Text("pitch"), new Text(attributes[19]));
        context.write(new Text("timbre"), new Text(attributes[20]));
        context.write(new Text("max_loudness"), new Text(attributes[21]));
        context.write(new Text("max_loudness_time"), new Text(attributes[22]));
        context.write(new Text("start_loudness"), new Text(attributes[23]));
        

    }
}

