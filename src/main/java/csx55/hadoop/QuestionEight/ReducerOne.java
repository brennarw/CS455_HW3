package csx55.hadoop.QuestionEight;

import java.io.IOException;
import java.util.PriorityQueue;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class ReducerOne extends Reducer<Text, Text, Text, Text> {

    //head is the most unique - high popularity low similarity
    //tail is the most generic - low popularity high similarity
    private PriorityQueue<ArtistObject> pq = new PriorityQueue<>();

    class ArtistObject implements Comparable<ArtistObject>{
        private String artistID;
        private double popularity; //familiarity + hottness
        private double similarity; //similar_artists + terms_freq

        public ArtistObject(String artistID, double popularity, double similarity){
            this.artistID = artistID;
            this.popularity = popularity;
            this.similarity = similarity;
        }
        
        @Override
        public int compareTo(ArtistObject other){
            int result = Double.compare(other.popularity, this.popularity); //sorts based on max popularity
    
            if(result == 0) { 
                return Double.compare(this.similarity, other.similarity); //sorts on min familiarity
            }
            return result;
            
        }
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        //NOTE: artist_terms_freq has brackets, similar_artists does not
        for(Text val : values){
            String[] songAttributes = val.toString().split("\\|");
            double popularity = Double.parseDouble(songAttributes[0]) + Double.parseDouble(songAttributes[1]); //familiarity + hottness

            String[] similarArtists = songAttributes[2].split(" ");
            String[] termFrequency = songAttributes[3].split(" ");

            double freqencyCount = 0;

            for(int i = 1; i < termFrequency.length - 1; i++){
                freqencyCount += Double.parseDouble(termFrequency[i]);
            }

            double similarity = similarArtists.length + freqencyCount;

            pq.add(new ArtistObject(key.toString(), popularity, similarity));

            break; //break because we only need to do this once for each artist, and each artist has its own reducer
        }

    }

    //cleanup runs only once after all reduce tasks have completed
    protected void cleanup(Context context) throws IOException, InterruptedException {

        ArtistObject unique = pq.poll();
        String uniqueVal = unique.artistID + "| popularity:" + String.valueOf(unique.popularity) + " | similarity:" + String.valueOf(unique.similarity); 
        context.write(new Text("Most Unique:"), new Text(uniqueVal));

        while(pq.peek() != null){
            if(pq.size() == 1){
                ArtistObject generic = pq.poll();
                String genericVal = generic.artistID + "| popularity:" + String.valueOf(generic.popularity) + " | similarity:" + String.valueOf(generic.similarity); 
                context.write(new Text("Most Generic:"), new Text(genericVal));
            }
            else {
                pq.remove();
            }
        }

    }
}