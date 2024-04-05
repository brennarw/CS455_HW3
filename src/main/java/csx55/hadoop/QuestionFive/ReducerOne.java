package csx55.hadoop.QuestionFive;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerOne extends Reducer<Text, IntWritable, Text, IntWritable> {

    private Text songIDLongest = new Text();
    private Text songIDShortest = new Text();
    private Text songIDMedian = new Text();
    private IntWritable longestDuration = new IntWritable();
    private IntWritable shortestDuration = new IntWritable(Integer.MAX_VALUE);
    private IntWritable medianDuration = new IntWritable();
    
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int duration = 0;

        for (IntWritable val : values) {
            duration += val.get();
        }

        // int loudnessAverage = loudness/songCount;
        
        if(duration > longestDuration.get()){
            songIDLongest.set(key);
            longestDuration.set(duration);
        }

        if(duration < shortestDuration.get()){
            songIDShortest.set(key);
            shortestDuration.set(duration);
        }

        int calculatedMedian = (longestDuration.get() + shortestDuration.get()) / 2; //will this calculate it with the updated longes/shortest durations?

        if(duration == calculatedMedian + 60 || duration == calculatedMedian - 60){
            songIDMedian.set(key);
            medianDuration.set(duration);
        }
        
    }
    
    //cleanup runs only once after all reduce tasks have completed
    protected void cleanup(Context context) throws IOException, InterruptedException {
        context.write(songIDLongest, longestDuration);
        context.write(songIDShortest, shortestDuration);
        context.write(songIDMedian, medianDuration);
    }
}