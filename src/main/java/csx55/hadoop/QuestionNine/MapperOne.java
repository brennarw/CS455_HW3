package csx55.hadoop.QuestionNine;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperOne extends Mapper<Object, Text, Text, Text> {

    private final static IntWritable one = new IntWritable(1); //key

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        String[] attributes = value.toString().split("\\|", 2);

        if(attributes[0].equals("SOAAXAK12A8C13C030")){
            context.write(new Text(attributes[0]), new Text(attributes[1]));
        }

    }
}

