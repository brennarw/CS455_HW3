package csx55.hadoop.QuestionTwo;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//uses both data sets
public class MapperOne extends Mapper<Object, Text, Text, IntWritable[]> {

    private final static IntWritable one = new IntWritable(1);
    private IntWritable loudness = new IntWritable();
    private Text artistID = new Text();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        //key = artistID
        //value = [songCount, loudness]
        String[] attributes = value.toString().split("\\|");

        artistID.set(attributes[0]); //TODO: set this to the correct index 
        loudness.set(Integer.parseInt(attributes[0])); //TODO: set this to the correct index - NOTE: might be a list of songs and loudness?

        IntWritable[] songSpecifics = new IntWritable[2];
        songSpecifics[0] = one;
        songSpecifics[1] = loudness;

        context.write(artistID, songSpecifics);

        

    }
}

