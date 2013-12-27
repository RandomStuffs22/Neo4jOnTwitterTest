/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package tweettest;

import java.io.IOException;
import java.text.ParseException;

/**
 *
 * @author funkboy
 */
public class TweetTest {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IOException, ParseException {
        //CreateGraph graph=new CreateGraph("Data - Riddick.csv");
        //CreateGraph.batchImportGraph("Data - Riddick.csv");
        CreateGraph.batchImportGraph("Processed_Data - Riddick.csv");
        CreateGraph.indexGraphDB();
        CreateGraph.dedupAndLink();
    }
}
