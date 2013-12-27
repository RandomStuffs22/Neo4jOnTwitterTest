/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package tweettest;


import org.neo4j.graphdb.Relationship;
import org.neo4j.helpers.collection.IterableWrapper;
import org.neo4j.graphdb.Path;
import java.util.Date;
import org.neo4j.graphdb.traversal.Traverser;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.kernel.Traversal;
import static tweettest.RelTypes.NEXT;
import static tweettest.RelTypes.TWEET;
import static tweettest.RelTypes.MENTION;
import static tweettest.RelTypes.REPLY_TO;
import static tweettest.RelTypes.RETWEET;

/**
 *
 * @author funkboy
 * Define a Tweet node, it stands for a single Tweet in Twitter
 */
public class Tweet {
    private final Node underlyingNode;
    static final String URL = "URL";
    static final String TEXT = "TEXT";
    static final String DATE = "DATE";
    
    public Tweet(Node tweetNode){
        underlyingNode = tweetNode;
    }
    
    public Node getUnderlyingNode(){
        return underlyingNode;
    }
    
    public User getUser(){
        return new User(getUserNode());
    }

    //get the User node of this Tweet node, it stands for the one who tweets the
    //tweet in Twitter
    private Node getUserNode() {
        TraversalDescription traversalDescription = Traversal.description().
                depthFirst().relationships(NEXT, Direction.INCOMING).
                relationships(TWEET, Direction.INCOMING).
                evaluator(Evaluators.includeWhereLastRelationshipTypeIs(TWEET));
        Traverser traverser = traversalDescription.traverse(getUnderlyingNode());
        return IteratorUtil.singleOrNull(traverser.iterator()).endNode();
    }
    
    public void setURL(String url){
        underlyingNode.setProperty(URL, url);
    }
    
    public String getURL(){
        return (String)underlyingNode.getProperty(URL);
    }
    
    public void setTweet(String text){
        underlyingNode.setProperty(TEXT, text);
    }
    
    public String getTweet(){
        return (String)underlyingNode.getProperty(TEXT);
    }
    
    public void setDate(String date){
        underlyingNode.setProperty(DATE, date);
    }
    
    public Date getDate() {
        Long l = (Long) underlyingNode.getProperty(DATE);
        return new Date(l);
    }
    
    //get all the User nodes mentioned by the Tweet node
    public Iterable<User> getMention(){
        Iterable<Relationship> rels = underlyingNode.getRelationships(MENTION);
        return createUserFromMention(rels);       
    }

    private Iterable<User> createUserFromMention(Iterable<Relationship> rels) {
        return new IterableWrapper<User, Relationship>(rels){

            @Override
            protected User underlyingObjectToObject(Relationship rel) {
                return new User(rel.getEndNode());
            }
        
        };
    }
    
    public Tweet getOriginTweet() {
        return new Tweet(underlyingNode.getSingleRelationship(RETWEET, Direction.OUTGOING).getEndNode());
    }
    
    public Iterable<Tweet> getRetweets(){
        Iterable<Relationship> rels = underlyingNode.getRelationships(RETWEET, Direction.INCOMING);       
        return createTweetFromIncomingRel(rels);
    }

    private Iterable<Tweet> createTweetFromIncomingRel(Iterable<Relationship> rels) {
        return new IterableWrapper<Tweet, Relationship>(rels) {

            @Override
            protected Tweet underlyingObjectToObject(Relationship rel) {
                return new Tweet(rel.getStartNode());
            }
        };
    }
    
    public Tweet getReplyTo(){
        return new Tweet(underlyingNode.getSingleRelationship(REPLY_TO, Direction.OUTGOING).getEndNode());
    }
    
    public Iterable<Tweet> getReplies(){
        Iterable<Relationship> rels = underlyingNode.getRelationships(RETWEET, Direction.INCOMING);       
        return createTweetFromIncomingRel(rels);
    }
    
    private IterableWrapper<User, Path> createUserFromPath(Traverser traverser) {
        return new IterableWrapper<User, Path>(traverser) {

            @Override
            protected User underlyingObjectToObject(Path path) {
                return new User(path.endNode());
            }
        };
    }
    
}
