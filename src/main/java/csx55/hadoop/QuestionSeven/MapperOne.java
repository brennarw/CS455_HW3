package csx55.hadoop.QuestionSeven;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperOne extends Mapper<Object, Text, IntWritable, Text> {

    private final static IntWritable one = new IntWritable(1); //key

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {


        

    }
}

