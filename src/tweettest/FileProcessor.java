/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package tweettest;

import au.com.bytecode.opencsv.CSVReader;
import au.com.bytecode.opencsv.CSVWriter;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import javax.json.JsonObject;
import twitterapitest.TwitterAPITest;

/**
 *
 * @author Liu Fan
 */
public class FileProcessor {

    private File input;
    private File output;

    FileProcessor(String path) {
        input = new File(path);
        output = new File("Processed_" + path);
    }

    public static void main(String[] args) throws Exception {
        FileProcessor fp = new FileProcessor("Data - Riddick.csv");
        fp.processCSV();
    }

    //remove Tweets not have value "en" in lang attribute
    public void processCSV() throws Exception {
        CSVReader cr = new CSVReader(new FileReader(input), ',', '\"', '\0', 1);
        CSVWriter cw = new CSVWriter(new FileWriter(output), '\t');
        ArrayList<String> keys = readKeyFile("keys.txt");

        if (keys.size() % 4 != 0) {
            System.out.println("keys file data incorrect!"+keys.size());
            return;
        }
        
        // round stores the number of keys pairs there are in the keys file
        int round = keys.size() / 4;

        String[] line;
        line = cr.readNext();

        String[] newline = new String[line.length + 1];
        String retweet_id = "";
        int length = line.length;
        String tweet_id = "";
        int count = 0;
        TwitterAPITest api = new TwitterAPITest(keys.get(count*4 + 0), keys.get(count*4 + 1),
                keys.get(count*4 + 2), keys.get(count*4 + 3));
        int total = 0;
        int calls = 0;
        Map list = new HashMap();
        while (line != null) {
            if (line[4].equals("en")) {
                total++;
                String[] mentions = new String[0];
                retweet_id = "";
                if (Integer.parseInt(line[5]) > 0 && line[3].startsWith("RT")) {
                    if (list.containsKey(line[3])) {
                        retweet_id = (String) list.get(line[3]);
                    } else {
                        tweet_id = line[0];
                        Map result = new HashMap();
                        if (calls < 180) {
                            Map out = api.getTweetByID(tweet_id);
                            JsonObject in = (JsonObject) out.get("object");
                            result = api.parseJson(in);
                        } else {
                            Map out = api.getTweetByIDAppOnly(tweet_id);
                            JsonObject in = (JsonObject)out.get("object");
                            result = api.parseJson(in);
                        }
                        if (result.containsKey("message0")) {
                            System.out.println(line[1] + "\t" + result.get("message0"));
                        } else {
                            retweet_id = (String) result.get("retweet_id");
                            list.put(line[3], retweet_id);
                        }
                        calls++;

                    }
                } else if (!line[9].equals("") && !line[3].startsWith("RT")) {
                    tweet_id = line[0];
                    Map result = new HashMap();
                    if (calls < 180) {
                        Map out = api.getTweetByID(tweet_id);
                            JsonObject in = (JsonObject) out.get("object");
                            result = api.parseJson(in);
                    } else {
                        Map out = api.getTweetByIDAppOnly(tweet_id);
                        JsonObject in = (JsonObject) out.get("object");
                        result = api.parseJson(in);
                    }
                    if (result.containsKey("message0")) {
                        System.out.println(line[1] + "\t" + result.get("message0"));
                    } else {
                        mentions = (String[]) result.get("mentions");
                    }
                    calls++;

                }
                System.arraycopy(line, 0, newline, 0, length);
                if (mentions.length > 0) {
                    String str = "";
                    for (int i = 0; i < mentions.length; i++) {
                        str += mentions[i];
                        if (i < mentions.length - 1) {
                            str += "|";
                        }
                    }
                    newline[9] = str;
                }
                newline[length] = retweet_id;
                cw.writeNext(newline);
                //System.out.println(total + "\t" + calls);

                if (calls == 360) {
                    count++;
                    System.out.println("using keys set: "+count);
                    if(count==round){
                        count=0;
                    }
                    api = new TwitterAPITest(keys.get(count*4 + 0), keys.get(count*4 + 1),
                            keys.get(count*4 + 2), keys.get(count*4 + 3));
                    calls = 0;
                }
                
            }
            line = cr.readNext();
        }
        cw.close();
    }

    public ArrayList<String> readKeyFile(String path) throws Exception {
        ArrayList<String> keys = new ArrayList();
        File f = new File(path);
        BufferedReader br = new BufferedReader(new FileReader(f));
        String line = br.readLine();
        // the key file save consumer key, consumer key secret, access token 
        //and access toke secret line by line sequentially

        while (line != null) {
            keys.add(line);
            line = br.readLine();
        }
        return keys;
    }
}
