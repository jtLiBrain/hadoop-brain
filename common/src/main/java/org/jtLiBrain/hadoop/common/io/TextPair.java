package org.jtLiBrain.hadoop.common.io;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TextPair implements WritableComparable<TextPair> {
    private Text first;
    private Text second;

    public TextPair() {
        this(new Text(), new Text());
    }

    public TextPair(Text first, Text second) {
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

    @Override
    public String toString() {
        return this.first + "\t" + this.second;
    }

    // used by the HashPartitioner (the default partitioner in MapReduce)
    @Override
    public int hashCode() {
        return this.first.hashCode() * 163 + this.second.hashCode();
    }

    public Text getSecond() {
        return this.second;
    }

    public Text getFirst() {
        return this.first;
    }

    /**
     *
     */
    public static class FullComparator extends WritableComparator {
        private static final Text.Comparator TEXT_COMPARATOR = new Text.Comparator();

        public FullComparator() {
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
                return TEXT_COMPARATOR.compare(
                        b1, s1 + firstL1, l1 - firstL1,
                        b2, s2 + firstL2, l2 - firstL2);
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

        public FirstComparator() {
            super(TextPair.class);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1,
                           byte[] b2, int s2, int l2) {
            try {
                int firstL1 = WritableUtils.decodeVIntSize(b1[s1]) + readVInt(b1, s1);
                int firstL2 = WritableUtils.decodeVIntSize(b2[s2]) + readVInt(b2, s2);
                return TEXT_COMPARATOR.compare(b1, s1, firstL1, b2, s2, firstL2);
            } catch (IOException e) {
                throw new IllegalArgumentException(e);
            }
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            if (a instanceof TextPair && b instanceof TextPair) {
                return ((TextPair) a).first.compareTo(((TextPair) b).first);
            }
            return super.compare(a, b);
        }
    }
}
