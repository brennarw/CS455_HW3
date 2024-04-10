package csx55.hadoop.QuestionTen;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperOne extends Mapper<Object, Text, Text, Text> {

    private final static IntWritable one = new IntWritable(1); //key
    private final static IntWritable year = new IntWritable(); //key

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        String[] attributes = value.toString().split("\\|");

        year.set(Integer.parseInt(attributes[44]));

        //System.out.println("year:" + attributes[44]);

        context.write(new Text("one"), new Text(attributes[44]));




    }
}

