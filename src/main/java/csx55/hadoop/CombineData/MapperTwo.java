package csx55.hadoop.CombineData;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//this deals with metadata.txt
public class MapperTwo extends Mapper<Object, Text, Text, Text> {

    private final static Text songID = new Text(); //key

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        String[] attributes = value.toString().split("\\|");

        songID.set(attributes[7]);

        String songValue = "M|";

        for(int i = 0; i < attributes.length; i++){
            if(i == 7){
                continue;
            }
            else{
                songValue += attributes[i] + "|";
            }
        }

        context.write(songID, new Text(songValue));

    }
}

