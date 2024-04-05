package csx55.hadoop.QuestionTwo;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerOne extends Reducer<Text, Text, Text, FloatWritable> {

    private Text artistID = new Text();
    private FloatWritable maxAverageLoudness = new FloatWritable();
    
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        float loudness = 0;
        float songCount = 0;

        for (Text val : values) {
            String[] songSpecifics = val.toString().split("\\|");
            songCount += Float.parseFloat(songSpecifics[0]);
            loudness += Float.parseFloat(songSpecifics[1]);
        }

        float loudnessAverage = loudness/songCount;
        
        if(loudnessAverage > maxAverageLoudness.get()){
            artistID.set(key);
            maxAverageLoudness.set(loudnessAverage);
        }
        
    }
    
    //cleanup runs only once after all reduce tasks have completed
    protected void cleanup(Context context) throws IOException, InterruptedException {
        context.write(artistID, maxAverageLoudness);
    }
}