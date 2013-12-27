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
