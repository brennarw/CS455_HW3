package csx55.hadoop.QuestionFive;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

//have two loops in the reducer: one loop that finds the max/min/median, and one loop that finds all other durations that match these values
//since they all have the same key, they will all be sent to the same reducer!
public class ReducerOne extends Reducer<IntWritable, Text, Text, FloatWritable> {

    private FloatWritable longestDuration = new FloatWritable();
    private FloatWritable shortestDuration = new FloatWritable(Integer.MAX_VALUE);
    private FloatWritable medianDuration = new FloatWritable();
    
    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        //first for loop finds the max/min/median values
        for (Text val : values) {
            float songDuration = Float.parseFloat(val.toString().split("\\|")[1]);

            if(songDuration > longestDuration.get()){
                longestDuration.set(songDuration);
            }

            if(songDuration < shortestDuration.get()){
                shortestDuration.set(songDuration);
            }
        }
        
        medianDuration.set((longestDuration.get() + shortestDuration.get()) / 2); //will this calculate it with the updated longes/shortest durations?
        //second for loop to write all songs that match these max/min/median values
        for(Text val: values){
            String[] keyValue = val.toString().split("\\|");
            float duration = Float.parseFloat(keyValue[0]);

            if(duration == longestDuration.get()){
                context.write(new Text(keyValue[0]), new FloatWritable(duration));
            }
            else if(duration == shortestDuration.get()){
                context.write(new Text(keyValue[0]), new FloatWritable(duration));
            }
            else if(duration == medianDuration.get() + 60 || duration == medianDuration.get() - 60){
                context.write(new Text(keyValue[0]), new FloatWritable(duration));
            }
        }
        
    }
}