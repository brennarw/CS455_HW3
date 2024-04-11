package csx55.hadoop.QuestionTwo;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//uses both data sets
public class MapperOne extends Mapper<Object, Text, Text, Text> {

    //private final static IntWritable one = new IntWritable(1);
    //private FloatWritable loudness = new FloatWritable();
    private Text artistID = new Text();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        //key = artistID
        //value = [songCount, loudness]
        String[] attributes = value.toString().split("\\|");

        String loudnessValue = "1|" + attributes[9];

        artistID.set(attributes[34]);  

        context.write(artistID, new Text(loudnessValue));

        

    }
}

