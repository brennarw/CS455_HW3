package csx55.hadoop.QuestionOne;

import java.io.IOException;

import javax.naming.Context;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerOne extends Reducer<Text, IntWritable, Text, IntWritable> {

    private Text artistID = new Text();
    private IntWritable maxSongCount = new IntWritable();
    
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int sum = 0;

        for (IntWritable val : values) {
            sum += val.get();
        }
        
        if(sum > maxSongCount.get()){
            artistID.set(key);
            maxSongCount.set(sum);
            //context.write(artistID, maxSongs);
        }
        
    }
    
    //cleanup runs only once after all reduce tasks have completed
    protected void cleanup(Context context) throws IOException, InterruptedException {
        context.write(artistID, maxSongCount);
    }
}