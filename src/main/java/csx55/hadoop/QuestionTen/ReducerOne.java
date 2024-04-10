package csx55.hadoop.QuestionTen;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class ReducerOne extends Reducer<Text, Text, Text, Text> {

    private final static IntWritable minYear = new IntWritable(Integer.MAX_VALUE); 
    private final static IntWritable maxYear = new IntWritable();

    //@Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        for(Text val : values){
            int year = Integer.parseInt(val.toString());
            if(year < minYear.get() && year != 0){
                minYear.set(year);
            }
            else if(year > maxYear.get()){
                maxYear.set(year);
            }
        }

        context.write(new Text("min year:"), new Text(String.valueOf(minYear.get())));
        context.write(new Text("max year:"), new Text(String.valueOf(maxYear.get())));

    }
}