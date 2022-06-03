package it.unipi.hadoop;
public class BitArray {

    private static final int ALL_ONES = 0xFFFFFFFF;
    private static final int WORD_SIZE = 32;
    private int bitsArray[] = null;
    private int k;
    private int m;
    private double fpr;

    public BitArray(int size, double fpr) {
        this.fpr = fpr;
        m = (int)Math.ceil(-Math.round((size * Math.log(fpr) / Math.pow(Math.log(2), 2))));
        int bitsIntSize = m / WORD_SIZE + (size % WORD_SIZE == 0 ? 0 : 1);
        bitsArray = new int[bitsIntSize];
        k = (int)Math.ceil(- Math.log(fpr) / Math.log(2));
    }

    public boolean getBit(int pos) {
        return (bitsArray[pos / WORD_SIZE] & (1 << (pos % WORD_SIZE))) != 0;
    }

    public void setBit(int pos, boolean b) {
        int word = bitsArray[pos / WORD_SIZE];
        int posBit = 1 << (pos % WORD_SIZE);
        if (b) {
            word |= posBit;
        } else {
            word &= (ALL_ONES - posBit);
        }
        bitsArray[pos / WORD_SIZE] = word;
    }

    public String getString() {
        String bitArrayString = "" + bitsArray[0];
        for(int i = 1; i < bitsIntSize; i++)
            bitArrayString += ";" + bitsArray[i];
        return bitArrayString;
    }

    public BitArray(String bitArrayString){
        String[] tokens = bitArrayString.split(";");

        bitsIntSize = tokens.length;
        bitsArray = new int[bitsIntSize];
        for(int i = 0; i < bitsIntSize; i++)
            bitsArray[i] = Integer.parseInt(tokens[i]);
    }
}
