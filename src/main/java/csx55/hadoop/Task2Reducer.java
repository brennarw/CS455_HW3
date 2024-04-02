package csx55.hadoop;

import java.io.IOException;

import javax.naming.Context;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Task2Reducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    
    private Text songID = new Text();
    private DoubleWritable maxLoudness = new DoubleWritable();
    
    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
      int sum = 0;
      for (DoubleWritable val : values) {
        sum += val.get();
      }
      if(sum > maxLoudness.get()){
        songID.set(key);
        maxLoudness.set(sum);
      }
    }

    //cleanup runs only once after all reduce tasks have completed
    protected void cleanup(Context context) throws IOException, InterruptedException {
        context.write(songID, maxLoudness);
      }
}
