package org.greg.server;

import java.util.Comparator;

public class Pair<A,B> {
    public final A first;
    public final B second;

    public Pair(A first, B second) {
        this.first = first;
        this.second = second;
    }

    public static <A,B> Comparator<Pair<A,B>> compareOnSecond(final Comparator<B> comparator) {
        return new Comparator<Pair<A, B>>() {
            public int compare(Pair<A, B> x, Pair<A, B> y) {
                return comparator.compare(x.second, y.second);
            }
        };
    }
}
