/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

import java.io.FileWriter;
import java.io.BufferedWriter;
import java.io.IOException;
import java.text.ParseException;
import org.neo4j.graphdb.Label;
import java.io.File;
import java.util.ArrayList;
import org.neo4j.graphdb.Relationship;
import tweettest.Tweet;
import tweettest.User;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import javax.json.JsonArray;
import javax.json.JsonObject;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import twitterapitest.UnLimitedRequest;
import static tweettest.RelTypes.MENTION;
import static tweettest.RelTypes.REPLY_TO;
import static tweettest.RelTypes.RETWEET;
import static tweettest.RelTypes.TWEET;
import static tweettest.RelTypes.NEXT;
import static tweettest.RelTypes.HASHTAG;
import static tweettest.RelTypes.MEDIA;


/**
 *
 * @author Liu Fan
 * this code shows how to use Neo4j Cypher API to operate on graph database
 */
public class GraphCypherTest {

    //
    private static String dbPath = "data/Riddick_GraphDB";
    private static String dbPath2 = "data/newSET";
    private static GraphDatabaseService graphDB;
    private static GraphDatabaseService graphDB2;
    private static ExecutionEngine engine;
    private static ExecutionEngine engine2;
    private static UnLimitedRequest ur;
    private static Label USER = DynamicLabel.label("USER");
    private static Label TW = DynamicLabel.label("TWEET");
    private static Label HT = DynamicLabel.label("HASHTAG");
    private static Label MD = DynamicLabel.label("MEDIA");
    
    public GraphCypherTest() {
    }

    @BeforeClass
    public static void setUpClass() throws Exception {
        graphDB = new GraphDatabaseFactory().newEmbeddedDatabase(dbPath);
        //graphDB2 = new GraphDatabaseFactory().newEmbeddedDatabase(dbPath2);
        engine = new ExecutionEngine(graphDB);
        //engine2 = new ExecutionEngine(graphDB2);
        ur = new UnLimitedRequest("keys.txt");
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        graphDB.shutdown();
        graphDB2.shutdown();
    }

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
    }

    @Test
    public void runQuery1() {
        String query = "match n:USER "
                + "return count( distinct n.ID)";
        ExecutionResult result = engine2.execute(query);
        printResult(result);

    }

    @Test
    public void runQuery2() {
        String query = "match n:HASHTAG "
                //+ "where n.RETWEETED=false "
                + "return count(n) ";
                //+"limit 50 ";
        ExecutionResult result = engine2.execute(query);
        printResult(result);
    }

    @Test
    public void runQuery3() {
        String query = "match n-[t:TWEET]->o "
                + "return count(t) as TWEET";
        ExecutionResult result = engine.execute(query);
        printResult(result);

    }

    @Test
    public void runQuery4() {
        String query = "match n-[t:MENTION]->o "
                + "return count(t) as MENTION";
        ExecutionResult result = engine.execute(query);
        printResult(result);

    }

    @Test
    public void runQuery5() {
        String query = "match n-[t:REPLY_TO]->o "
                + "return count(t) as REPLY";
        ExecutionResult result = engine.execute(query);
        printResult(result);

    }

    @Test
    public void runQuery6() {
        String query = "match n-[t:NEXT]->o "
                + "return count(t) as NEXT";
        ExecutionResult result = engine.execute(query);
        printResult(result);

    }

    @Test
    public void runQuery7() {
        String query = "match n-[t:RETWEET]->o "
                + "return count(t) as RT";
        ExecutionResult result = engine.execute(query);
        printResult(result);

    }

    @Test
    public void runQuery8() {
        String query = "match n-[t]->o "
                + "return count(t) as ALL";
        ExecutionResult result = engine.execute(query);
        printResult(result);

    }

    ///////////////////////////////////////////////////////////////////////////////////////////////
    @Test
    public void runQuery9() {
        String query = "match n:TWEET "
                + "where (n.RETWEETS>0) and (n.REWEETCOUNT_UPDATE=null) "
                + "return n.URL";
        ExecutionResult result = engine.execute(query);
        ResourceIterator<Map<String, Object>> iter = result.iterator();
        Map<String, Object> rec;
        Map<String, Object> res;
        JsonObject obj = null;
        String url = "";
        int code = 0;
        int retweetCount = 0;
        int count = 0;
        while (iter.hasNext()) {
            rec = iter.next();
            url = (String) rec.get("n.URL");
            try {
                res = ur.getTweetByID(url);
                code = (Integer) res.get("code");
                if (code == 200) {
                    obj = (JsonObject) res.get("object");
                    retweetCount = obj.getInt("retweet_count");
                } else {
                    System.out.println(code + "   " + url);
                    continue;
                }
            } catch (Exception e) {
                System.out.println("UNE   " + url);
                continue;
            }
            try (Transaction tx = graphDB.beginTx()) {
                Node Tweet = graphDB.findNodesByLabelAndProperty(TW, "URL", url).iterator().next();
                Tweet.setProperty("REWEETCOUNT_UPDATE", retweetCount);
                count++;
                tx.success();
            }
        }
        System.out.println(count);
    }
    
    @Test
    public void runQuery10() {
        String query = "match n:TWEET, p-[:TWEET]->()-[:NEXT*0..]->n "
                + "where n.RETWEETS>10 "
                + "return p.NAME, p.FOLLOWERS, p.FRIENDS, n.RETWEETS, n.REWEETCOUNT_UPDATE, n.TEXT, n.DATE, p.ID "
                + "order by n.RETWEETS desc";
        ExecutionResult result = engine.execute(query);
        printResult(result);
    }

    @Test
    public void runQuery11() {
//        File f = new File("TUserID.txt");
//        ArrayList<String> ID = new ArrayList();
//        try (BufferedReader br = new BufferedReader(new FileReader(f))) {
//            String line = br.readLine();
//            while (line != null) {
//                if (!ID.contains(line)) {
//                    ID.add(line);
//                }
//                line = br.readLine();
//            }
//        } catch (Exception e) {
//            System.out.println("check text file!");
//        }
//        System.out.println(ID.size());

        String query = "match n:USER "
                + "return n.ID "
                + "skip 3622"
                + "limit 2000";
        ExecutionResult result = engine.execute(query);
        ResourceIterator<Map<String, Object>> iter = result.iterator();
        Map<String, Object> rec;
        Map<String, Object> res;
        long user_id = 0;
        //String user_name = "";
        String max_id = null;
        String since_id = null;
        int code = 0;
        JsonArray arry;
        JsonObject tw;
        long tw_id = 0;
        String text;
        String date;
        boolean retweeted;
        JsonArray hashtags;
        JsonArray media;
        JsonArray user_mentions;
        JsonObject entities;
        Date dateTw;
        int retweet;
        int size;
        ArrayList<JsonObject> tweets;
        int request = 0;
        Date stop = new Date(113, 0, 1);
        while (iter.hasNext()) {
            //for (int i = 0; i < ID.size(); i++) {
            rec = iter.next();
            user_id = (long) rec.get("n.ID");
            tweets = new ArrayList();
            System.out.println(user_id);
            int count = 0;
            max_id = null;
            try {
                do {
                    res = ur.getTimelineByUserID(String.valueOf(user_id), max_id, since_id);
                    request++;
                    code = (Integer) res.get("code");
                    size = 0;
                    if (code == 200) {
                        arry = (JsonArray) res.get("object");
                        size = arry.size();
                        int j = 0;
                        for (; j < size; j++) {
                            tw = arry.getJsonObject(j);
                            tweets.add(tw);
                            count++;
                        }
                        if (size > 0) {
                            JsonObject obj = arry.getJsonObject(size - 1);
                            date = obj.getString("created_at");
                            dateTw = new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy").parse(date);
                            if (dateTw.before(stop)) {
                                break;
                            }
                            tw_id = Long.parseLong(obj.getString("id_str"));
                            //long l = Long.parseLong(tw_id);
                            //l--;
                            max_id = String.valueOf(tw_id - 1);
                        }
                    } else {
                        System.out.println(code + "   " + user_id);
                    }
                } while (size != 0);
            } catch (Exception e) {
                System.out.println("UNE   " + user_id + " " + request);
            }
            System.out.println("...." + count);
            Node previousNode;
            try (Transaction tx = graphDB2.beginTx()) {
                if (tweets.size() > 0) {
                    Node node = graphDB2.createNode(USER);
                    node.setProperty("ID", user_id);
                    previousNode = node;
                    for (int j = 0; j < tweets.size(); j++) {
                        JsonObject obj = tweets.get(j);
                        tw_id = Long.parseLong(obj.getString("id_str"));
                        Node tweet = graphDB2.createNode();
                        tweet.addLabel(TW);
                        text = obj.getString("text");
                        date = obj.getString("created_at");
                        dateTw = new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy").parse(date);
                        retweet = obj.getInt("retweet_count");
                        retweeted = obj.containsKey("retweeted_status");
                        entities = obj.getJsonObject("entities");
                        hashtags = entities.getJsonArray("hashtags");
                        media = entities.getJsonArray("media");
                        user_mentions = entities.getJsonArray("user_mentions");

                        tweet.setProperty("URL", tw_id);
                        tweet.setProperty("TEXT", text);
                        tweet.setProperty("DATE", dateTw.toString());
                        tweet.setProperty("RETWEETS", retweet);
                        tweet.setProperty("RETWEETED", retweeted);
                        //Tweet t= new Tweet(tweet);
                        if (hashtags != null) {
                            if (!hashtags.isEmpty()) {
                                for (int k = 0; k < hashtags.size(); k++) {
                                    String hashtag_text = ((JsonObject) hashtags.get(k)).getString("text");
                                    Node hashtag = graphDB2.createNode(HT);
                                    hashtag.setProperty("TEXT", hashtag_text);
                                    tweet.createRelationshipTo(hashtag, HASHTAG);
                                }
                            }
                        }
                        if (media != null) {
                            if (!media.isEmpty()) {
                                for (int k = 0; k < media.size(); k++) {
                                    String type = ((JsonObject) media.get(k)).getString("type");
                                    Node md = graphDB2.createNode(MD);
                                    md.setProperty("TYPE", type);
                                    tweet.createRelationshipTo(md, MEDIA);
                                }
                            }
                        }
                        if (user_mentions != null) {
                            if (!user_mentions.isEmpty()) {
                                for (int k = 0; k < user_mentions.size(); k++) {
                                    long id = Long.parseLong(((JsonObject) user_mentions.get(k)).getString("id_str"));
                                    Node usr = graphDB2.createNode(USER);
                                    usr.setProperty("ID", id);
                                    tweet.createRelationshipTo(usr, MENTION);
                                }
                            }
                        }
                        if (previousNode.hasLabel(USER)) {
                            previousNode.createRelationshipTo(tweet, TWEET);
                        } else {
                            previousNode.createRelationshipTo(tweet, NEXT);
                        }
                        previousNode = tweet;
                        //user.addTweet(t);
                    }

                }
                tx.success();
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
        }
    }
    
    
    

    
    @Test
    public void runQuery12() throws Exception {
        String query = "match n:TWEET "
                +"with count(n.URL) as count, n.URL as URL "
                +"where count>1 "
                +"return count, URL";
        ExecutionResult result = engine.execute(query);
        ResourceIterator<Map<String, Object>> iter = result.iterator();
        while (iter.hasNext()) {
            Map<String, Object> rec = iter.next();
            String url = (String) rec.get("URL");
            query = "match n:TWEET, n<-[:NEXT*0..]-()<-[:TWEET]-p "
                    + "where n.URL=" + "'" + url + "' "
                    + "return n, p.ID, n.URL";
            ExecutionResult r = engine.execute(query);
            ResourceIterator<Map<String, Object>> i = r.iterator();
            while(i.hasNext()){
                Map<String, Object> record = i.next();
                System.out.println(record.get("n").toString()+"\t"+record.get("p.ID").toString()+"\t"+record.get("n.URL").toString());
            }
        }
    }
    
    

    @Test
    public void runQuery14() throws Exception {
        String query = "match t:TWEET "
                + "where t.TEXT=null "
                + "return t.URL";
        ExecutionResult result = engine.execute(query);
        ResourceIterator<Map<String, Object>> iter = result.iterator();
        Map<String, Object> rec;
        String url = "";
        String text = "";

        JsonObject obj;
        int code = 0;
        Map<String, Object> res;

        while (iter.hasNext()) {
            rec = iter.next();
            url = rec.get("t.URL").toString();
            res = ur.getTweetByID(url);
            code = (Integer) res.get("code");
            obj = (JsonObject) res.get("object");

            if (code != 200) {
                System.out.println(url + "   " + code);
                continue;
            }
            try (Transaction tx = graphDB.beginTx()) {
                ResourceIterator<Node> n = graphDB.findNodesByLabelAndProperty(TW, "URL", url).iterator();
                Node tw = n.next();
                n.close();
                text = obj.getString("text");
                tw.setProperty("TEXT", text);


                String date = obj.getString("created_at");
                Date dateTw = new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy").parse(date);
                tw.setProperty("DATE", dateTw.toString());

                JsonArray mentions = ((JsonObject) obj.getJsonObject("entities")).getJsonArray("user_mentions");
                if (!mentions.isEmpty()) {
                    for (int i = 0; i < mentions.size(); i++) {
                        String mention = ((JsonObject) mentions.getJsonObject(i)).getString("screen_name");
                        User me = getOrCreateUser(mention);
                        tw.createRelationshipTo(me.getUnderlyingNode(), MENTION);
                    }
                }

                String author = ((JsonObject) obj.getJsonObject("user")).getString("screen_name");
                User auth = getOrCreateUser(author);
                auth.addTweet(new Tweet(tw));


                if (!obj.isNull("in_reply_to_status_id_str")) {
                    String reply = obj.getString("in_reply_to_status_id_str");
                    traceReply(tw, reply);
                }
                tx.success();

            }
        }
    }

    
    @Test
    public void runQuery15() {
        String query = "match n:USER "
                + "where n.NAME=null "
                + "return n.ID ";
        ExecutionResult result = engine2.execute(query);
        ResourceIterator<Map<String, Object>> iter = result.iterator();
        Map<String, Object> rec;
        long user_id = 0;
        JsonObject obj = null;
        int code = 0;
        Map<String, Object> res;
        int count = 0;
        String name = "";
        int followers = 0;
        int friends = 0;
        boolean verified = false;
        while (iter.hasNext()) {
            rec = iter.next();
            user_id = (long) rec.get("n.ID");
            try {
                res =ur.getUserByID(String.valueOf(user_id));
                code = (Integer) res.get("code");
                if (code != 200) {
                    System.out.println(code + "   " + user_id);
                    continue;
                }
                obj = (JsonObject) res.get("object");
                if (obj == null) {
                    System.out.println("901" + "   " + user_id);
                    continue;
                }

                if (!obj.containsKey("followers_count")) {
                    System.out.println("900" + "   " + user_id);
                    continue;
                }
                if (!obj.isNull("followers_count")) {
                    followers = obj.getInt("followers_count");
                } else {
                    System.out.println("911" + "   " + user_id);
                }

                if (!obj.containsKey("friends_count")) {
                    System.out.println("999" + "   " + user_id);
                    continue;
                }
                if (!obj.isNull("friends_count")) {
                    friends = obj.getInt("friends_count");
                } else {
                    System.out.println("912" + "   " + user_id);
                }

                name = obj.getString("screen_name");
                verified = obj.getBoolean("verified");
            } catch (Exception e) {
                System.out.println("UNE   " + user_id);
                continue;
            }

            try (Transaction tx = graphDB2.beginTx()) {
                Node user = graphDB2.findNodesByLabelAndProperty(USER, "ID", user_id).iterator().next();
                //user.setProperty("ID", id);
                user.setProperty("FOLLOWERS", followers);
                user.setProperty("FRIENDS", friends);
                user.setProperty("VERIFIED", verified);
                user.setProperty("NAME", name);
                count++;
                tx.success();
            }
        }
        System.out.println(count);
    }

    
    @Test
    public void runQuery16() {
        String query = "match n:USER, p=n-[:TWEET]-t "
                + "where n.ID=null "
                + "return t.URL,n.NAME ";
        ExecutionResult result = engine.execute(query);
        ResourceIterator<Map<String, Object>> iter = result.iterator();
        String url = "";
        String name = "";
        long id = 0;
        int followers = 0;
        int friends = 0;
        boolean verified = false;
        Map<String, Object> res;
        int code = 0;
        JsonObject jObj;
        int count = 0;
        String name2 = "";
        while (iter.hasNext()) {
            Map rec = iter.next();
            url = (String) rec.get("t.URL");
            name = (String) rec.get("n.NAME");
            try {
                res = ur.getTweetByID(url);
                code = (Integer) res.get("code");
                if (code == 200) {
                    jObj = (JsonObject) res.get("object");
                    jObj = jObj.getJsonObject("user");
                    id = jObj.getInt("id");
                    followers = jObj.getInt("followers_count");
                    friends = jObj.getInt("friends_count");
                    verified = jObj.getBoolean("verified");
                    name2 = jObj.getString("screen_name");
                } else {
                    System.out.println(code + "   " + url + "   " + name);
                    continue;
                }
            } catch (Exception e) {
                System.out.println("Exp   " + url + "   " + name);
                continue;
            }
            try (Transaction tx = graphDB.beginTx()) {
                Node user = getOrCreateUser(name).getUnderlyingNode();
                user.setProperty("ID", id);
                user.setProperty("NAME", name2);
                user.setProperty("FOLLOWERS", followers);
                user.setProperty("FRIENDS", friends);
                user.setProperty("VERIFIED", verified);
                count++;
                tx.success();
            }
        }
        System.out.println(count);
    }

    @Test
    public void runQuery17() {
        try (Transaction tx = graphDB.beginTx()) {
            Iterator<Node> nodes = graphDB.getAllNodes().iterator();
            Node tweet;
            int count = 0;
            while (nodes.hasNext()) {
                tweet = nodes.next();
                if (tweet.hasLabel(DynamicLabel.label("TWEET"))) {
                    count = 0;
                    Iterator<Relationship> rels = tweet.getRelationships(RETWEET, Direction.INCOMING).iterator();
                    while (rels.hasNext()) {
                        count++;
                        rels.next();
                    }
                    tweet.setProperty("RETWEETS", count);
                }
            }
            tx.success();
        }
    }

    public void traceReply(Node tw, String url) throws Exception {
        ResourceIterator<Node> n = graphDB.findNodesByLabelAndProperty(TW, "URL", url).iterator();
        if (n.hasNext()) {
            Node re = n.next();
            tw.createRelationshipTo(re, REPLY_TO);
            n.close();
        } else {
            Map<String, Object> res = ur.getTweetByID(url.toString());
            int code = (Integer) res.get("code");
            JsonObject obj = (JsonObject) res.get("object");
            if (code == 200) {
                Node re = graphDB.createNode(TW);
                re.setProperty("URL", url);
                String text = obj.getString("text");
                re.setProperty("TEXT", text);
                String date = obj.getString("created_at");
                Date dateTw = new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy").parse(date);
                re.setProperty("DATE", dateTw.toString());
                JsonArray mentions = ((JsonObject) obj.getJsonObject("entities")).getJsonArray("user_mentions");
                if (!mentions.isEmpty()) {
                    for (int i = 0; i < mentions.size(); i++) {
                        String mention = ((JsonObject) mentions.getJsonObject(i)).getString("screen_name");
                        User me = getOrCreateUser(mention);
                        re.createRelationshipTo(me.getUnderlyingNode(), MENTION);
                    }
                }

                String author = ((JsonObject) obj.getJsonObject("user")).getString("screen_name");
                User auth = getOrCreateUser(author);
                auth.addTweet(new Tweet(re));
                tw.createRelationshipTo(re, REPLY_TO);

                if (!obj.isNull("in_reply_to_status_id_str")) {
                    String reply = obj.getString("in_reply_to_status_id_str");
                    traceReply(tw, reply);
                }
            }

        }
    }

    public static User getOrCreateUser(String name) {
        User user;
        try (ResourceIterator<Node> iter = graphDB.findNodesByLabelAndProperty(USER, "NAME", name).iterator()) {
            user = null;
            if (iter.hasNext()) {
                user = new User(iter.next());
            } else {
                Node u = graphDB.createNode();
                u.addLabel(USER);
                u.setProperty("NAME", name);
                user = new User(u);
            }
        }
        return user;
    }
    
    public static void deleteUser(Node n) {
        Iterator<Relationship> rels = n.getRelationships().iterator();
        System.out.println("Deleting...User Node:" + n.getId() + "  " + n.getProperty("ID") );
        while (rels.hasNext()) {
            Relationship rel = rels.next();
            if (rel.isType(TWEET)) {
                Node tweet = rel.getEndNode();    
                rel.delete();
                delectTweetChain(tweet);
            } else {
                rel.delete();
            }
            
        }    
        n.delete();
    }
    
    public static void delectTweetChain(Node n) {
        Iterator<Relationship> rels = n.getRelationships().iterator();
        System.out.println("\tDeleting...Tweet Node:" + n.getId() + "  "  + n.getProperty("URL"));
        try{
            while (rels.hasNext()) {
            Relationship rel = rels.next();
            if (rel.isType(NEXT)&&rel.getStartNode().equals(n)) {
                Node tweet = rel.getEndNode();
                rel.delete();
                delectTweetChain(tweet);
            } else {
                rel.delete();
            }     
        } 
        n.delete();
        }catch(Exception e){
            System.out.println(n.getId()+"   "+"OK");
        }
    }

    public void printResult(ExecutionResult result) {
        System.out.println(result.dumpToString());
    }
    
    @Test
    public void createIndex(){
        String query="create index on :USER(ID)";
        ExecutionResult result = engine2.execute(query);
        printResult(result);
        query = "create index on :HASHTAG(TEXT)";
        result = engine2.execute(query);
        printResult(result);
        query = "create index on :MEDIA(TYPE)";
        result = engine2.execute(query);
        printResult(result);        
    }
    
    @Test
    public void deDup() {
        String query = "match n:HASHTAG "
                + "with n.TEXT as TEXT, count(n.TEXT) as count "
                + "where count>1 "
                + "return TEXT, count";
        ExecutionResult result = engine2.execute(query);
        //printResult(result);


        ResourceIterator<Map<String, Object>> iter = result.iterator();
        Map<String, Object> rec;
        String text = "";
        while (iter.hasNext()) {
            rec = iter.next();
            text = (String) rec.get("TEXT");
            System.out.println("Deduplicating "+text);
            try (Transaction tx = graphDB2.beginTx()) {
                Iterator<Node> nodes = graphDB2.findNodesByLabelAndProperty(HT, "TEXT", text).iterator();
                Node ht1 = nodes.next();
                Node ht2;
                while (nodes.hasNext()) {
                    ht2 = nodes.next();
                    Iterator<Relationship> rels = ht2.getRelationships().iterator();
                    Relationship rel;
                    Node startNode;
                    while (rels.hasNext()) {
                        rel = rels.next();
                        startNode = rel.getStartNode();
                        startNode.createRelationshipTo(ht1, rel.getType());
                        rel.delete();
                    }
                    ht2.delete();
                }
                tx.success();
            }
            System.out.println("Deduplicated "+text);
        }

    }
    
    
    @Test
    public void test1() throws ParseException, IOException {
        File file1 = new File("daily_native_tweet.txt");
        File file2 = new File("daily_retweet_count.txt");
        File file3 = new File("daily_re_tweet.txt");
        File file4 = new File("users_data.txt");
        file1.createNewFile();
        file2.createNewFile();
        file3.createNewFile();
        file4.createNewFile();
        BufferedWriter bw1 = new BufferedWriter(new FileWriter(file1));
        BufferedWriter bw2 = new BufferedWriter(new FileWriter(file2));
        BufferedWriter bw3 = new BufferedWriter(new FileWriter(file3));
        BufferedWriter bw4 = new BufferedWriter(new FileWriter(file4));
        try (Transaction tx = graphDB2.beginTx()) {
            Iterator<Node> nodes = graphDB2.getAllNodes().iterator();
            Node user = null;
            Node tweet = null;
            Date before = new Date(113, 0, 1);
            Date after = new Date(113, 10, 1);
            int[][] daily_native_tweet = new int[11][31];
            int[][] daily_re_tweet = new int[11][31];
            int[][] daily_retweet_num = new int[11][31];
            
            
            long id = 0;
            String screen_name = "";
            int followers = 0;
            int friends = 0;
            boolean verified = false;

            //String url = "";
            String text = "";
            int retweets = 0;
            String date = "";
            Date time = null;
            int day = 0;
            int month = 0;
            int year = 0;
            boolean retweeted = false;
            Iterator<Relationship> relIter = null;

            double ttl_retweet_num = 0;
            double ttl_native_tweet = 0;
            double ttl_re_tweet=0;
            double diffusion_rate = 0;
            int users=0;
            boolean flag=false;
            while (nodes.hasNext()) {
                user = nodes.next();
                if(user.getId()==21334778){
                    flag=true;
                    user=nodes.next();
                }
                if(flag==false){
                    continue;
                }
                if (user.hasLabel(USER)) {
                    id = (long) user.getProperty("ID");
                    screen_name = (String) user.getProperty("NAME");
                    followers = (int) user.getProperty("FOLLOWERS");
                    friends = (int) user.getProperty("FRIENDS");
                    verified = (boolean) user.getProperty("VERIFIED");
                    tweet = user.getRelationships(TWEET).iterator().next().getEndNode();
                    ttl_retweet_num = 0;
                    ttl_native_tweet = 0;
                    ttl_re_tweet = 0;
                    for (int i = 0; i < 11; i++) {
                        for (int j = 0; j < 31; j++) {
                            daily_native_tweet[i][j] = 0;
                        }
                    }

                    for (int i = 0; i < 11; i++) {
                        for (int j = 0; j < 31; j++) {
                            daily_re_tweet[i][j] = 0;
                        }
                    }

                    for (int i = 0; i < 11; i++) {
                        for (int j = 0; j < 31; j++) {
                            daily_retweet_num[i][j] = 0;
                        }
                    }
                    do {
                        
                        //url = (String) tweet.getProperty("URL");
                        text = (String) tweet.getProperty("TEXT");
                        retweets = (int) tweet.getProperty("RETWEETS");
                        date = (String) tweet.getProperty("DATE");
                        time = new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy").parse(date);
                        retweeted = (boolean)tweet.getProperty("RETWEETED");
                        if (time.after(after)) {
                            relIter = tweet.getRelationships(Direction.OUTGOING, NEXT).iterator();
                            if (relIter.hasNext()) {
                                tweet = relIter.next().getEndNode();
                            } else {
                                break;
                            }
                            continue;
                        }
                        if (time.before(before)) {
                            break;
                        }
                        
                        year = time.getYear() + 1900;
                        month = time.getMonth();
                        day = time.getDate();
                        if (retweeted) {
                            ttl_re_tweet++;
                            daily_re_tweet[month][day - 1]++;
                        } else {
                            ttl_native_tweet++;
                            ttl_retweet_num += retweets;
                            daily_native_tweet[month][day - 1]++;
                            daily_retweet_num[month][day - 1] += retweets;
                        }
                                                
                        relIter = tweet.getRelationships(Direction.OUTGOING, NEXT).iterator();
                        if (relIter.hasNext()) {
                            tweet = relIter.next().getEndNode();
                        } else {
                            break;
                        }
                    } while (true);
                    int activeDays= getNumOfActiveDays(daily_native_tweet, daily_re_tweet)-27;
                    double daily_re_tweets = ttl_re_tweet/activeDays;
                    double daily_native_tweets = ttl_native_tweet/activeDays;
                    double native_tweet_ratio = ttl_native_tweet/(ttl_native_tweet+ttl_re_tweet);
                    double retweet_count_per_tweet = ttl_retweet_num/ttl_native_tweet;
                    String str = id+","+screen_name+","+followers+","+friends+","+verified
                            +","+daily_native_tweets+","+","+daily_re_tweets+","+native_tweet_ratio
                            +","+retweet_count_per_tweet;
//                    System.out.print(id + "\t" + screen_name + "\t" + ttl_native_tweet + "\t" + ttl_re_tweet + "\t" + ttl_retweet_num + 
//                            "\t" + diffusion_rate + "\t" + followers + "\t" + friends);
                    String str1=""+id;
                    String str2=""+id;
                    String str3=""+id;
                    for(int i=0;i<11;i++){
                        for(int j=0;j<31;j++){
                        str1+=","+daily_native_tweet[i][j];
                        str2+=","+daily_retweet_num[i][j];
                        str3+=","+daily_re_tweet[i][j];
                        }
                    }
                    bw1.write(str1);
                    bw1.newLine();
                    bw1.flush();
                    bw2.write(str2);
                    bw2.newLine();
                    bw2.flush();
                    bw3.write(str3);
                    bw3.newLine();
                    bw3.flush();
                    bw4.write(str);
                    bw4.newLine();
                    bw4.flush();
                    
                    
                    users++;
                    if(users%100==0){
                    System.out.println("================="+users+"================");
                    }
                }
                
            }
            tx.success();
        }
        bw1.close();
        bw2.close();
        bw3.close();
        bw4.close();
    }
    
    @Test
    public void test2() {
        try (Transaction tx = graphDB2.beginTx()) {
            Node user = graphDB2.findNodesByLabelAndProperty(USER, "ID", "91478624").iterator().next();
            Node tweet = user.getRelationships(TWEET).iterator().next().getEndNode();
            Iterator<Relationship> relIter;
            do {
                relIter = tweet.getRelationships(NEXT, Direction.OUTGOING).iterator();
                if (relIter.hasNext()) {
                    tweet = relIter.next().getEndNode();
                } else {
                    System.out.println((String) tweet.getProperty("URL") + "\t" + (String) tweet.getProperty("DATE")+ "\t" + (String) tweet.getProperty("TEXT"));
                    break;
                }
            } while (true);
            tx.success();
        }
    }
    
    @Test
    public void testDeleteUserAndTweets(){
        try(Transaction tx = graphDB2.beginTx()){
            Node user = graphDB2.findNodesByLabelAndProperty(USER, "ID", -2142999476).iterator().next();
            deleteUser(user);
            tx.success();
        }
    }
    
    
    @Test
    public void cc(){
    String query= "match n:MEDIA "
                + "with n.TYPE as TYPE, count(n.TYPE) as count "
                + "where count>1 "
                + "return TYPE, count";
        ExecutionResult result = engine2.execute(query);
        printResult(result);
    }
    
    @Test
    public void deleteAllMentions() {
//        String query= "match n:USER, n<-[r:MENTION] "
//                +"delete n, r";
//        ExecutionResult result = engine2.execute(query);
//        printResult(result);

        //int count = 0;
        Iterator<Node> nodes;
        try (Transaction tx = graphDB2.beginTx()) {
            nodes = graphDB2.getAllNodes().iterator();
            tx.success();
        }
        for (int i=0; i<20; i++) {
            try (Transaction tx = graphDB2.beginTx()) {
                Node user;
                Relationship rel;
                int count = 0;
                while (nodes.hasNext()) {
                    user = nodes.next();
                    if (user.hasLabel(USER)) {
                        rel = user.getSingleRelationship(MENTION, Direction.BOTH);
                        if (rel != null) {
                            rel.delete();
                            user.delete();
                            count++;
                            if (count == 200000) {
                                System.out.println(count);
                                break;
                            }
                        }

                    }
                }
                tx.success();
            }
        }
    }
    
    
    
    public int getNumOfActiveDays(int[][] a, int[][] b) {
        int activeDays = 0;
        int length = 0;
        for (int i = 0; i < a.length; i++) {
            length += a[i].length;
        }

        int[] c = new int[length];
        int n = 0;
        //boolean startActive = false;
        outer:
        for (int i = 0; i < a.length; i++) {
            for (int j = 0; j < a[i].length; j++) {
                c[n] = a[i][j] + b[i][j];
                if (c[n] != 0) {
                    //startActive = true;
                    break outer;
                }
                n++;
            }

        }
        activeDays = length - n;
        return activeDays;
    }
    
    @Test
    public void tt(){
        String query="start n=node(21334778) "
                +"return n";
        ExecutionResult result= engine2.execute(query);
        printResult(result);
        query = "match n:USER "
                + "where n.NAME=null "
                + "return n.ID ";
        result= engine2.execute(query);
        printResult(result);
    }
    
}
