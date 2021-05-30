package org.jtLiBrain.hadoop.common.io;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Partitioner;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TextPair implements WritableComparable<TextPair> {
    private static final Text.Comparator TEXT_COMPARATOR = new Text.Comparator();

    private Text first;
    private Text second;

    public TextPair() {
        this(new Text(), new Text());
    }

    public TextPair(Text first, Text second) {
        this.first = first;
        this.second = second;
    }

    @Override
    public int compareTo(TextPair o) {
        int cmp = this.first.compareTo(o.first);
        if (cmp != 0) {
            return cmp;
        }
        return this.second.compareTo(o.second);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        this.first.write(out);
        this.second.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.first.readFields(in);
        this.second.readFields(in);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof TextPair) {
            TextPair tp = (TextPair) obj;
            return this.first.equals(tp.first) && this.second.equals(tp.second);
        }
        return false;
    }

    // used by the HashPartitioner (the default partitioner in MapReduce)
    @Override
    public int hashCode() {
        return this.first.hashCode() * 163 + this.second.hashCode();
    }

    @Override
    public String toString() {
        return this.first + "\t" + this.second;
    }

    // getters and setters
    public Text getFirst() {
        return first;
    }

    public void setFirst(Text first) {
        this.first = first;
    }

    public Text getSecond() {
        return second;
    }

    public void setSecond(Text second) {
        this.second = second;
    }

    public void set(Text first, Text second) {
        this.first = first;
        this.second = second;
    }

    public void set(String first, String second) {
        this.first = new Text(first);
        this.second = new Text(second);
    }

    /**
     * This comparator uses the natural ordering for the first key and then for the second key.
     */
    public static class NaturalComparator extends WritableComparator {
        public NaturalComparator() {
            super(TextPair.class);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1,
                           byte[] b2, int s2, int l2) {
            try {
                int firstL1 = WritableUtils.decodeVIntSize(b1[s1]) + readVInt(b1, s1);
                int firstL2 = WritableUtils.decodeVIntSize(b2[s2]) + readVInt(b2, s2);
                int cmp = TEXT_COMPARATOR.compare(b1, s1, firstL1, b2, s2, firstL2);
                if (cmp != 0) {
                    return cmp;
                }

                cmp = TEXT_COMPARATOR.compare(
                        b1, s1 + firstL1, l1 - firstL1,
                        b2, s2 + firstL2, l2 - firstL2);

                return cmp;
            } catch (IOException e) {
                throw new IllegalArgumentException(e);
            }
        }
    }

    /**
     * Partition based on the first part of the pair.
     */
    public static class FirstPartitioner<V> extends Partitioner<TextPair, V> {
        @Override
        public int getPartition(TextPair key, V value,
                                int numPartitions) {
            return Math.abs(key.getFirst().hashCode() % numPartitions);
        }
    }

    /**
     * Compare only the first part of the pair, so that reduce is called once
     * for each value of the first part.
     */
    public static class FirstGroupingComparator implements RawComparator<TextPair> {
        @Override
        public int compare(byte[] b1, int s1, int l1,
                           byte[] b2, int s2, int l2) {
            try {
                int firstL1 = WritableUtils.decodeVIntSize(b1[s1]) + WritableComparator.readVInt(b1, s1);
                int firstL2 = WritableUtils.decodeVIntSize(b2[s2]) + WritableComparator.readVInt(b2, s2);

                return TEXT_COMPARATOR.compare(
                        b1, s1, firstL1,
                        b2, s2, firstL2);
            } catch (IOException e) {
                throw new IllegalArgumentException(e);
            }
        }

        @Override
        public int compare(TextPair o1, TextPair o2) {
            return o1.getFirst().compareTo(o2.getFirst());
        }
    }
}
