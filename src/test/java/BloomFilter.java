
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.hash.MurmurHash;

public class BloomFilter {

    private static final int ALL_ONES = 0xFFFFFFFF;
    private static final int WORD_SIZE = 32;
    private int[] bitsArray = null;
    private int m_arrayWordsSize = 0;
    private int k_hashFunctionNumber = 0;
    private int n_itemNumber = 0;
    private int m_arrayBitsSize = 0;
    private double p_falsePositiveRate = 0.0;

    public BloomFilter(int n_itemNumber, double p_falsePositiveRate) {
        this.p_falsePositiveRate = p_falsePositiveRate;
        this.n_itemNumber = n_itemNumber;
        m_arrayBitsSize = (int)Math.ceil(- n_itemNumber * Math.log(p_falsePositiveRate) / Math.log(2) / Math.log(2));
        k_hashFunctionNumber = (int)Math.ceil(m_arrayBitsSize / n_itemNumber * Math.log(2));
        m_arrayWordsSize = m_arrayBitsSize / WORD_SIZE + (m_arrayBitsSize % WORD_SIZE == 0 ? 0 : 1);
        bitsArray = new int[m_arrayWordsSize];
    }

    public boolean checkItem(Text item) {
        int bitPosition,posBit,mh,i;
        for(i = 0; i < k_hashFunctionNumber; i++) {
            mh = new MurmurHash().hash(item.getBytes(), item.getBytes().length, i);
            bitPosition = ((mh % m_arrayBitsSize) + m_arrayBitsSize) % m_arrayBitsSize;
            posBit = 1 << (bitPosition % WORD_SIZE);
            if((bitsArray[bitPosition / WORD_SIZE] & posBit) == 0)
                return false;
        }
        return true;
    }

    public void addItem(Text item) {
        int bitPosition,posBit,mh,i;
        for(i = 0; i < k_hashFunctionNumber; i++) {
            mh = new MurmurHash().hash(item.getBytes(), item.getBytes().length, i);
            bitPosition = ((mh % m_arrayBitsSize) + m_arrayBitsSize) % m_arrayBitsSize;
            posBit = 1 << (bitPosition % WORD_SIZE);
            bitsArray[bitPosition / WORD_SIZE] |= posBit;
        }
    }

    public String getString() {
        String bitArrayString = "" + bitsArray[0];
        for(int i = 1; i < m_arrayWordsSize; i++)
            bitArrayString += ";" + bitsArray[i];
        return bitArrayString;
    }

    public void setString(String integerList){
        String[] tokens = integerList.split(";");
        for(int i = 0; i < m_arrayWordsSize; i++)
            bitsArray[i] = Integer.parseInt(tokens[i]);
    }
}
