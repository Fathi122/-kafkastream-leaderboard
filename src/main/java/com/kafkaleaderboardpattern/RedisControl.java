package com.kafkaleaderboardpattern;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Tuple;

import java.util.Iterator;
import java.util.Set;

public class RedisControl implements Runnable{
    public static final String ANSI_RESET = "\u001B[0m";
    public static final String ANSI_BLUE = "\u001B[34m";

    private static String sortedSetKey = "voter-leaderboard";
    private Jedis jedisDriver;

    //Create a connection and reset the leaderboard
    public void setUp() {
        try{
            //Jedis running on localhost and port 6379
            jedisDriver =new Jedis("localhost");
            //reset the sorted set key
            jedisDriver.del(sortedSetKey);
            System.out.println("Redis connection setup successfully");
        }
        catch(Exception e) {
            e.printStackTrace();
        }
    }
    // update vote score
    public void update_vote_score(String product, double count) {
        jedisDriver.zincrby(sortedSetKey,count,product);
    }

    @Override
    public void run() {
        try {
            while (true) {

                //Query the leaderboard and print the results
                Set<Tuple> scores=
                        jedisDriver.zrevrangeWithScores(
                                sortedSetKey,0,-1);

                Iterator<Tuple> iScores = scores.iterator();
                int position=1;

                while (iScores.hasNext()) {
                    Tuple score= iScores.next();
                    System.out.println(
                            new StringBuilder().append(ANSI_BLUE).append("Leaderboard - ").append(position).append(" : ").append(score.getElement()).append(" = ").append(score.getScore()).append(ANSI_RESET).toString());
                    position++;
                }

                Thread.currentThread().sleep(5000);
            }
        }
        catch(Exception e) {
            e.printStackTrace();
        }
    }
}
