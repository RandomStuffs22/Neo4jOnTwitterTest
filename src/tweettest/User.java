/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package tweettest;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.helpers.collection.PositionedIterator;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.traversal.TraversalDescription;
import java.util.Collections;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.helpers.collection.IterableWrapper;
import org.neo4j.kernel.Traversal;
import static tweettest.RelTypes.MENTION;
import static tweettest.RelTypes.TWEET;
import static tweettest.RelTypes.NEXT;
/**
 *
 * @author funkboy
 * Define a User node, it stands for a single user in Twitter
 */
public class User {
    
    static final String NAME="NAME";
    
    private final Node underlyingNode;
    
    public User(Node userNode){
        underlyingNode=userNode;
    }
    
    public Node getUnderlyingNode(){
        return underlyingNode;
    }
    
    public String getName(){
        return (String)underlyingNode.getProperty(NAME);
    }
    
    @Override
    public int hashCode(){
        return underlyingNode.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof User && underlyingNode.equals(((User)obj).getUnderlyingNode());
    }
    
    @Override
    public String toString(){
        return "User["+getName()+"]";
    }
    
    public Iterable<Tweet> getMentionedTweets(){
        Iterable<Relationship> rels=underlyingNode.getRelationships(MENTION, Direction.INCOMING);       
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
    
    public void addTweet(Tweet tweet) throws ParseException {
        Tweet oldTweet = null;
        PositionedIterator<Tweet> tweets = new PositionedIterator(getTweets().iterator());
        if (!tweets.hasNext()) {
            underlyingNode.createRelationshipTo(tweet.getUnderlyingNode(), TWEET);
            return;
        }
        while (tweets.hasNext()) {
            Date dateTw=new SimpleDateFormat("EEE MMM dd HH:mm:ss zzzz yyyy").
                    parse((String)tweet.getUnderlyingNode().getProperty(Tweet.DATE));
            Date dateOl=new SimpleDateFormat("EEE MMM dd HH:mm:ss zzzz yyyy").
                    parse((String)tweets.next().getUnderlyingNode().getProperty(Tweet.DATE));
            if (dateTw.after(dateOl)) {
                oldTweet = tweets.current();
                break;
            }
        }
        if (oldTweet == null) {
            oldTweet = tweets.current();
            oldTweet.getUnderlyingNode().createRelationshipTo(tweet.getUnderlyingNode(), NEXT);
        } else {
            Relationship rel = oldTweet.getUnderlyingNode().getRelationships(Direction.INCOMING).iterator().next();
            RelationshipType type = (RelationshipType) rel.getType();
            Node previousNode = rel.getStartNode();
            rel.delete();
            previousNode.createRelationshipTo(tweet.getUnderlyingNode(), type);
            tweet.getUnderlyingNode().createRelationshipTo(oldTweet.getUnderlyingNode(), NEXT);
        }
    }
    
    public Iterable<Tweet> getTweets() {
        Relationship latestTweet = underlyingNode.getSingleRelationship(TWEET, Direction.OUTGOING);
        if (latestTweet == null) {
            return Collections.emptyList();
        }

        TraversalDescription traversal = Traversal.description().depthFirst().
                relationships(NEXT);

        return new IterableWrapper<Tweet, Path>(traversal.traverse(latestTweet.getEndNode())) {

            @Override
            protected Tweet underlyingObjectToObject(Path path) {
                return new Tweet(path.endNode());
            }
        };
    }
    
}
