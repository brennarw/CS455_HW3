package csx55.hadoop.QuestionNine;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class ReducerOne extends Reducer<Text, Text, Text, Text> {


    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        for(Text val : values){
            String[] attributes = val.toString().split("\\|");

            context.write(new Text("song:"), key); //TODO: need to come up with a new songID
            context.write(new Text("artist:"), new Text(attributes[33])); //TODO: need to come up with a new artistID
            context.write(new Text("hottness:"), new Text(attributes[0]));
            context.write(new Text("tempo:"), new Text(attributes[12]));
            context.write(new Text("time signature:"), new Text(attributes[13]));
            context.write(new Text("danceability"), new Text("1.0")); //this value was changed
            context.write(new Text("duration:"), new Text(attributes[3]));
            context.write(new Text("mode:"), new Text(attributes[9]));
            context.write(new Text("energy:"), new Text("0.5")); //this value was changed
            context.write(new Text("key:"), new Text(attributes[6]));
            context.write(new Text("loudness:"), new Text(attributes[8]));
            context.write(new Text("stops fading in at:"), new Text(attributes[4]));
            context.write(new Text("starts fading in at:"), new Text(attributes[11]));
            context.write(new Text("artist terms:"), new Text(attributes[40]));
        }
        

    }
}