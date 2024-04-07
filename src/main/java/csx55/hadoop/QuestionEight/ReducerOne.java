package csx55.hadoop.QuestionEight;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class ReducerOne extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        class ArtistObject implements Comparable<ArtistObject>{
            private String artistID;
            private float popularity;
            private float similarity;
    
            public ArtistObject(String artistID, float popularity, float similarity){
                this.artistID = artistID;
                this.popularity = popularity;
                this.similarity = similarity;
            }
            
            @Override
            public int compareTo(ArtistObject other){
                int result = Float.compare(other.popularity, this.popularity); //sorts based on max popularity
        
                if(result == 0) { 
                    return Float.compare(this.similarity, other.similarity); //sorts on min familiarity
                }
                return result;
                
            }
        }
    }
}