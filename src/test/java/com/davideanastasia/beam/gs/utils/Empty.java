package com.davideanastasia.beam.gs.utils;


public class Empty implements Comparable<Empty> {
    private Empty() {}

    @Override
    public int compareTo(Empty o) {
        return 0;
    }

    @Override
    public boolean equals(Object aThat) {
        if (this == aThat) return true;
        if (!(aThat instanceof Empty)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return 0;
    }

    public static final Empty EMPTY = new Empty();
}
