package com.ibeifeng.sparkproject;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;


/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        System.out.println( "Hello World!" );
        Random random = new Random();
        System.out.println(String.valueOf(random.nextInt(100)));

        Map<String, Long> map = new HashMap<String, Long>();
        map.put("123", 1L);
        Long k = map.get("333");
        System.out.println(k);
    }


}
