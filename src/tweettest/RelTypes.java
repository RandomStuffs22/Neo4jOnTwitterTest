/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package tweettest;

import org.neo4j.graphdb.RelationshipType;

/**
 *
 * @author funkboy
 * Define the relations between the nodes
 */
public enum RelTypes implements RelationshipType {

    A_PERSON,
    TWEET,
    NEXT,
    MENTION,
    REPLY_TO,
    RETWEET,
    HASHTAG,
    MEDIA,
}
