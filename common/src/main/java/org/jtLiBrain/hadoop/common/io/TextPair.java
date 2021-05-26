package org.jtLiBrain.hadoop.common.io;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Partitioner;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TextPair implements WritableComparable<TextPair> {
    private Text first;
    private Text second;

    private TextPair() {}

    public TextPair(Text first, Text second) {
        assert first != null;
        assert second != null;

        this.first = first;
        this.second = second;
    }

    // imposes the ordering
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
        return this.first;
    }
    public Text getSecond() {
        return this.second;
    }

    /**
     *
     */
    public static class FullComparator extends WritableComparator {
        private static final Text.Comparator TEXT_COMPARATOR = new Text.Comparator();

        private boolean isAsc = true;

        public FullComparator(boolean isAsc) {
            super(TextPair.class);
            this.isAsc = isAsc;
        }

        @Override
        public int compare(byte[] b1, int s1, int l1,
                           byte[] b2, int s2, int l2) {
            try {
                int firstL1 = WritableUtils.decodeVIntSize(b1[s1]) + readVInt(b1, s1);
                int firstL2 = WritableUtils.decodeVIntSize(b2[s2]) + readVInt(b2, s2);
                int cmp = TEXT_COMPARATOR.compare(b1, s1, firstL1, b2, s2, firstL2);
                if (cmp != 0) {
                    if(isAsc)
                        return cmp;
                    else
                        return cmp * -1;
                }

                cmp = TEXT_COMPARATOR.compare(
                        b1, s1 + firstL1, l1 - firstL1,
                        b2, s2 + firstL2, l2 - firstL2);

                if(isAsc)
                    return cmp;
                else
                    return cmp * -1;
            } catch (IOException e) {
                throw new IllegalArgumentException(e);
            }
        }
    }

    /**
     *
     */
    public static class FirstComparator extends WritableComparator {
        private static final Text.Comparator TEXT_COMPARATOR = new Text.Comparator();

        private boolean isAsc = true;

        public FirstComparator(boolean isAsc) {
            super(TextPair.class);
            this.isAsc = isAsc;
        }

        @Override
        public int compare(byte[] b1, int s1, int l1,
                           byte[] b2, int s2, int l2) {
            try {
                int firstL1 = WritableUtils.decodeVIntSize(b1[s1]) + readVInt(b1, s1);
                int firstL2 = WritableUtils.decodeVIntSize(b2[s2]) + readVInt(b2, s2);

                if(isAsc)
                    return TEXT_COMPARATOR.compare(b1, s1, firstL1, b2, s2, firstL2);
                else
                    return TEXT_COMPARATOR.compare(b1, s1, firstL1, b2, s2, firstL2) * -1;
            } catch (IOException e) {
                throw new IllegalArgumentException(e);
            }
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            int cmp;
            if (a instanceof TextPair && b instanceof TextPair) {
                cmp = ((TextPair) a).first.compareTo(((TextPair) b).first);
            } else {
                cmp = super.compare(a, b);
            }

            if(isAsc)
                return cmp;
            else
                return cmp * -1;
        }
    }


    /**
     * Partition based on the first part of the pair.
     */
    public static class FirstPartitioner<V> extends Partitioner<TextPair, V> {
        @Override
        public int getPartition(TextPair key, V value, int numPartitions) {
            return Math.abs(key.getFirst().hashCode() % numPartitions);
        }
    }

    /**
     * Compare only the first part of the pair, so that reduce is called once
     * for each value of the first part.
     */
    public static class FirstGroupingComparator extends WritableComparator {
        private boolean isAsc = true;

        public FirstGroupingComparator(boolean isAsc) {
            super(TextPair.class, true);
            this.isAsc = isAsc;
        }

        @Override
        public int compare(WritableComparable wc1, WritableComparable wc2) {
            TextPair ck1 = (TextPair) wc1;
            TextPair ck2 = (TextPair) wc2;

            int cmp = ck1.getFirst().compareTo(ck2.getFirst());

            if(isAsc)
                return cmp;
            else
                return cmp * -1;
        }
    }
}
