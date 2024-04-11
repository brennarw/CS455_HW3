package csx55.hadoop.QuestionTen;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperOne extends Mapper<Object, Text, Text, Text> {
    
    private final static Text songYear = new Text();

    /*have 9 reducers
    * reducer 1 key: 1926
    * reducer 2 key: 1936
    * reducer 3 key: 1946
    * reducer 4 key: 1956
    * reducer 5 key: 1966
    * reducer 6 key: 1976
    * reducer 7 key: 1986
    * reducer 8 key: 1996
    * reducer 9 key: 2006
    */

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        String[] attributes = value.toString().split("\\|");

        int year = Integer.parseInt(attributes[44]);
        //attributes[41] is artist terms

        if(year != 0 && year < 1936){
            songYear.set("1926");
        }
        else if(year != 0 && year < 1946){
            songYear.set("1936");
        }
        else if(year != 0 && year < 1956){
            songYear.set("1946"); 
        }
        else if(year != 0 && year < 1966){
            songYear.set("1956"); 
        }
        else if(year != 0 && year < 1976){
            songYear.set("1966"); 
        }
        else if(year != 0 && year < 1986){
            songYear.set("1976");
        }
        else if(year != 0 && year < 1996){
            songYear.set("1986");
        }
        else if(year != 0 && year < 2006){
            songYear.set("1996"); 
        }
        else if(year != 0 && year <= 2010){
            songYear.set("2006"); 
        }

        String songTempoDuration = "1|" + attributes[13] + "|" + attributes[4]; //1|tempo|duration

        context.write(songYear, new Text(songTempoDuration));

    }
}

