package org.greg.examples;

import org.greg.client.Greg;

public class Main {
    public static void main(String[] args) throws InterruptedException {
        for(int i = 0; ; ++i) {
            Greg.log("Hello from raw api #" + i);
            Thread.sleep(100);
        }
    }
}
