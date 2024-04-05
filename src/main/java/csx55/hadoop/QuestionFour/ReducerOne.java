package csx55.hadoop.QuestionFour;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerOne extends Reducer<Text, IntWritable, Text, IntWritable> {

    private Text artistID = new Text();
    private IntWritable maxFadingTime = new IntWritable();
    
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int totalFadingTime = 0;

        for(IntWritable val : values){
            totalFadingTime += val.get();
        }

        if(totalFadingTime > maxFadingTime.get()){
            artistID.set(key);
            maxFadingTime.set(totalFadingTime);
        }
        
    }
    
    //cleanup runs only once after all reduce tasks have completed
    protected void cleanup(Context context) throws IOException, InterruptedException {
        context.write(artistID, maxFadingTime);
    }
}