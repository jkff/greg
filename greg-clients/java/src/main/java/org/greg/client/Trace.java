package org.greg.client;

class Trace {
    public static boolean ENABLED = Boolean.parseBoolean(System.getProperty("greg.trace.enabled", "false"));

    public static void writeLine(String s) {
        if (ENABLED) {
            System.err.println(s);
        }
    }

    public static void writeLine(String s, Exception e) {
        if (ENABLED) {
            System.err.println(s);
            e.printStackTrace();
        }
    }
}
