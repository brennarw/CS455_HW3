package csx55.hadoop.QuestionOne;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//uses metadat.txt
public class MapperOne extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text artist = new Text();

    //this mapper needs to find each unique artistID and write each instance found to the reducer
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        //String[] lines = value.toString().split("\n"); //each line represents one artist
        
        //for(String line : lines){
            //String[] attributes = value.toString().split("\\|");
            artist.set(value.toString().split("\\|")[2]); //artistID is the third attribute in the line
            context.write(artist, one); //this is what writes each <key, value> pair to the reducer
        //}
    }
}