package csx55.hadoop.QuestionFour;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerOne extends Reducer<Text, FloatWritable, Text, FloatWritable> {

    private Text artistID = new Text();
    private FloatWritable maxFadingTime = new FloatWritable();
    
    @Override
    protected void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {

        float totalFadingTime = 0;

        for(FloatWritable val : values){
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