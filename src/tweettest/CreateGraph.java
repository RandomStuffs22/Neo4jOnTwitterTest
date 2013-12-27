/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package tweettest;


import java.text.SimpleDateFormat;
import java.util.Date;
import org.neo4j.graphdb.Relationship;
import java.util.Iterator;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import java.util.Map;
import java.util.HashMap;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import java.text.ParseException;
import au.com.bytecode.opencsv.CSVReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.kernel.impl.util.FileUtils;
import org.neo4j.unsafe.batchinsert.BatchInserters;
import static tweettest.RelTypes.TWEET;
import static tweettest.RelTypes.MENTION;
import static tweettest.RelTypes.REPLY_TO;
import static tweettest.RelTypes.NEXT;
import static tweettest.RelTypes.RETWEET;

/**
 *
 * @author funkboy
 * Create a graph database by Neo4j on Twitter data
 */
public class CreateGraph {
    //the path of the created graph database
    private static String dbPath = "data/Riddick_GraphDB";
    //the original csv file contains twitter data
    private static String importCsv = "";
    private static GraphDatabaseService graphDB;
    private static Label userLabel = DynamicLabel.label("USER");
    private static Label tweetLabel = DynamicLabel.label("TWEET");
    private static ExecutionEngine engine;
    
    
    //create the graph database on the imported data and index the User node and Tweet node
    CreateGraph(String path) throws IOException, ParseException {
        importCsv = path;
        FileUtils.deleteRecursively(new File(dbPath));
        graphDB = new GraphDatabaseFactory().newEmbeddedDatabase(dbPath);
        try (Transaction tx = graphDB.beginTx()) {
            IndexDefinition userIndex = graphDB.schema().indexFor(userLabel).on(User.NAME).create();
            IndexDefinition tweetIndex = graphDB.schema().indexFor(tweetLabel).on(Tweet.URL).create();
            tx.success();
        }
        CSVReader csvReader = new CSVReader(new FileReader(importCsv), ',', '\"', '\0', 1);
        String[] row = null;
        int count = 0;
        try (Transaction tx = graphDB.beginTx()) {
            while ((row = csvReader.readNext()) != null) {
                count++;
                if (row.length < 10) {
                    System.out.println(count);
                    continue;
                }

                Tweet tweet = getOrCreateTweet(row[0]);
                Node tweetNode = tweet.getUnderlyingNode();
                tweetNode.setProperty(Tweet.DATE, row[2]);
                tweetNode.setProperty(Tweet.TEXT, row[3]);
                String name = row[6];
                User user = getOrCreateUser(name);
                user.addTweet(tweet);

                String replyTo = row[9];
                if (!replyTo.equals("")) {
                    Tweet replyTweet = getOrCreateTweet(replyTo);
                    tweetNode.createRelationshipTo(replyTweet.getUnderlyingNode(), REPLY_TO);
                }

                String mentions = row[7];
                if (!mentions.equals("")) {
                    String[] users = mentions.split(",");
                    for (int i = 0; i < users.length; i++) {
                        String mentionedName = users[i];
                        User mentionedUser = getOrCreateUser(mentionedName);
                        tweetNode.createRelationshipTo(mentionedUser.getUnderlyingNode(), MENTION);
                    }
                }
                if (count % 1000 == 0) {
                    System.out.println(count + " tweets have been processed...");
                }
            }
            csvReader.close();
            tx.success();
        }
        graphDB.shutdown();
    }

    //create a graph database in batch mode, it is fast
    //but it cannot guarantee the uniquness the nodes, deduplication is needed afterwards
    public static void batchImportGraph(String csv) throws IOException, ParseException {
        importCsv = csv;
        FileUtils.deleteRecursively(new File(dbPath));
        BatchInserter inserter = BatchInserters.inserter(dbPath);
        CSVReader csvReader = new CSVReader(new FileReader(importCsv), '\t', '\"', '\0');
        String[] row = null;
        int count = 0;
        Map<String, Object> tweetProp = new HashMap<>();
        Map<String, Object> userProp = new HashMap<>();
        while ((row = csvReader.readNext()) != null) {
            
            count++;
            tweetProp.put(Tweet.URL, row[0]);
            Date dateTw=new SimpleDateFormat("yyyy-MM-dd HH:mm:ssX").parse(row[2]);
            tweetProp.put(Tweet.DATE, dateTw.toString());
            tweetProp.put(Tweet.TEXT, row[3]);
            long tweet = inserter.createNode(tweetProp, tweetLabel);
            userProp.put(User.NAME, row[6]);
            long user = inserter.createNode(userProp, userLabel);
            inserter.createRelationship(user, tweet, TWEET, null);
            if (!row[8].equals("")) {
                tweetProp = new HashMap<>();
                tweetProp.put(Tweet.URL, row[8]);
                long reply = inserter.createNode(tweetProp, tweetLabel);
                inserter.createRelationship(tweet, reply, REPLY_TO, null);
            }
            String mentions = row[9];
            if (!mentions.equals("")) {
                String[] users = mentions.split("\\|");
                for (int i = 0; i < users.length; i++) {
                    userProp.put(User.NAME, users[i]);
                    long mention = inserter.createNode(userProp, userLabel);
                    inserter.createRelationship(tweet, mention, MENTION, null);
                }
            }
            
            if (!row[16].equals("")) {
                tweetProp = new HashMap();
                tweetProp.put(Tweet.URL, row[16]);
                long retweet = inserter.createNode(tweetProp, tweetLabel);
                inserter.createRelationship(tweet, retweet, RETWEET, null);
            }
            
            if (count % 1000 == 0) {
                System.out.println(count + " tweets have been processed...");
            }
        }
        inserter.shutdown();

    }
    
    //create indexes for User node and Tweet node
    public static void indexGraphDB() {
        graphDB = new GraphDatabaseFactory().newEmbeddedDatabase(dbPath);
        try (Transaction tx = graphDB.beginTx()) {
            IndexDefinition userIndex = graphDB.schema().indexFor(userLabel).on(User.NAME).create();
            IndexDefinition tweetIndex = graphDB.schema().indexFor(tweetLabel).on(Tweet.URL).create();
            tx.success();
        }
        try (Transaction tx = graphDB.beginTx()) {
            graphDB.schema().awaitIndexesOnline(10, TimeUnit.MINUTES);
            System.out.println("indexing finished...");
            tx.success();
        }
        graphDB.shutdown();
    }
    
    //deduplication after batch mode import
    public static void dedupAndLink() throws ParseException{
        graphDB = new GraphDatabaseFactory().newEmbeddedDatabase(dbPath);
        engine = new ExecutionEngine(graphDB);
        dedupTweetNode();
        dedupUserNode();
        graphDB.shutdown();
    }

    //deduplicate User node 
    public static void dedupTweetNode() {
        String query = "start n=node(*) "
                + "return count(n.URL)as count,n.URL "
                + "order by count desc";
        ExecutionResult result = engine.execute(query);
        ResourceIterator<Map<String, Object>> iter = result.iterator();
        while (iter.hasNext()) {
            Map<String, Object> rec = iter.next();
            String url = (String) rec.get("n.URL");
            long count = (long) rec.get("count");
            if (count > 1) {
                try (Transaction tx = graphDB.beginTx()) {
                    ResourceIterator<Node> tweets = graphDB.findNodesByLabelAndProperty(tweetLabel, Tweet.URL, url).iterator();
                    Node t = tweets.next();
                    Tweet tweet = new Tweet(t);
                    if (t.hasProperty(Tweet.TEXT)) {
                        tweet.setTweet((String) t.getProperty(Tweet.TEXT));
                        tweet.setDate((String) t.getProperty(Tweet.DATE));
                    }
                    while (tweets.hasNext()) {
                        t = tweets.next();
                        if (t.hasProperty(Tweet.TEXT)) {
                            tweet.setTweet((String) t.getProperty(Tweet.TEXT));
                            tweet.setDate((String) t.getProperty(Tweet.DATE));
                        }
                        Iterator<Relationship> rels = t.getRelationships().iterator();
                        while (rels.hasNext()) {
                            Relationship rel = rels.next();
                            String type = rel.getType().name();
                            if (type.equals(MENTION.name())) {
                                Node p = rel.getEndNode();
                                tweet.getUnderlyingNode().createRelationshipTo(p, MENTION);
                            } else if (type.equals(TWEET.name())) {
                                Node p = rel.getStartNode();
                                p.createRelationshipTo(tweet.getUnderlyingNode(), TWEET);
                            } else {
                                Node r = rel.getStartNode();
                                if (r.getProperty(Tweet.URL).equals(t.getProperty(Tweet.URL))) {
                                    r = rel.getEndNode();
                                    tweet.getUnderlyingNode().createRelationshipTo(r, rel.getType());
                                } else {
                                    r.createRelationshipTo(tweet.getUnderlyingNode(), rel.getType());
                                }
                            } 
                            rel.delete();
                        }
                        t.delete();
                    }
                    tx.success();
                }
            } else {
                iter.close();
                break;
            }
        }

    }
    
    
    //deduplicate Tweet node
    public static void dedupUserNode() throws ParseException {
        String query = "start n=node(*) "
                + "return count(n.NAME)as count,n.NAME "
                + "order by count desc";
        ExecutionResult result = engine.execute(query);
        ResourceIterator<Map<String, Object>> iter = result.iterator();
        while (iter.hasNext()) {
            Map<String, Object> rec = iter.next();
            String name = (String) rec.get("n.NAME");
            long count = (long) rec.get("count");
            if (count > 1) {
                try (Transaction tx = graphDB.beginTx()) {
                    ResourceIterator<Node> users = graphDB.findNodesByLabelAndProperty(userLabel, User.NAME, name).iterator();
                    Node u = users.next();
                    User user = new User(u);
                    while (users.hasNext()) {
                        u = users.next();
                        Iterator<Relationship> rels = u.getRelationships().iterator();
                        while (rels.hasNext()) {
                            Relationship rel = rels.next();
                            String type = rel.getType().name();
                            if (type.equals(TWEET.name())) {
                                Node t = rel.getEndNode();
                                Tweet tweet=new Tweet(t);
                                user.addTweet(tweet);                           
                            } else if (type.equals(MENTION.name())) {
                                Node t = rel.getStartNode();
                                t.createRelationshipTo(user.getUnderlyingNode(), MENTION);
                            } 
                            rel.delete();
                        }
                        u.delete();
                    }
                    tx.success();
                }
            } else {
                iter.close();
                break;
            }
        }

    }
    
    public static User getOrCreateUser(String name) {
        ResourceIterator<Node> iter = graphDB.findNodesByLabelAndProperty(userLabel, User.NAME, name).iterator();
        User user = null;
        if (iter.hasNext()) {
            user = new User(iter.next());
        } else {
            Node u = graphDB.createNode();
            u.addLabel(userLabel);
            u.setProperty(User.NAME, name);
            user = new User(u);
        }
        iter.close();
        return user;
    }

    public static Tweet getOrCreateTweet(String url) {
        ResourceIterator<Node> iter = graphDB.findNodesByLabelAndProperty(tweetLabel, Tweet.URL, url).iterator();
        Tweet tweet = null;
        if (iter.hasNext()) {
            tweet = new Tweet(iter.next());
        } else {
            Node u = graphDB.createNode();
            u.addLabel(tweetLabel);
            u.setProperty(Tweet.URL, url);
            tweet = new Tweet(u);
        }
        iter.close();
        return tweet;
    }
    
    public static void deleteUser(Node n) {
        Iterator<Relationship> rels = n.getRelationships().iterator();
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
    }
}
