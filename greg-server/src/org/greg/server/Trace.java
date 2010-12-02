package org.greg.server;

public class Trace {
    public static void writeLine(String s) {
        System.out.println(s);
    }
    public static void writeLine(String s, Exception e) {
        System.out.println(s);
        e.printStackTrace();
    }
}
