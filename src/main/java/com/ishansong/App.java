package com.ishansong;

import java.util.UUID;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {

        System.out.println(UUID.randomUUID().toString().replaceAll("-",""));
    }
}
