package com.ibeifeng.sparkproject;

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
    }


}
