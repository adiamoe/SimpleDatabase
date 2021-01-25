package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

    private static final ConcurrentHashMap<String, TableStats> statsMap = new ConcurrentHashMap<String, TableStats>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(HashMap<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException | IllegalAccessException | IllegalArgumentException | SecurityException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

    private int ioCostPerPage;
    private DbFile table;
    private int numTuples;
    private int numPages;
    private HashMap<Integer, IntHistogram> intHistograms;
    private HashMap<Integer, StringHistogram> stringHistograms;

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        // Done
        table = Database.getCatalog().getDatabaseFile(tableid);
        intHistograms = new HashMap<>();
        stringHistograms = new HashMap<>();
        this.ioCostPerPage = ioCostPerPage;
        numTuples = 0;
        int numField = table.getTupleDesc().numFields();

        DbFileIterator iter = table.iterator(new TransactionId());
        HashMap<Integer, Integer> maxField = new HashMap<>();
        HashMap<Integer, Integer> minField = new HashMap<>();
        for(int i=0; i<numField; ++i)
        {
            if(table.getTupleDesc().getFieldType(i).equals(Type.INT_TYPE))
            {
                maxField.put(i, Integer.MIN_VALUE);
                minField.put(i, Integer.MAX_VALUE);
            }
            else {
                stringHistograms.put(i, new StringHistogram(NUM_HIST_BINS));
            }
        }
        try
        {
            iter.open();
            //获取每个attribute的最大最小值，用于构建histogram
            while(iter.hasNext())
            {
                Tuple tup = iter.next();
                for(int i=0; i<numField; ++i)
                {
                    IntField field = (IntField) tup.getField(i);
                    int value = field.getValue();
                    if(value > maxField.get(i))
                        maxField.put(i, value);
                    if(value < minField.get(i))
                        minField.put(i, value);
                }
                numTuples++;
            }
            for(int i=0; i<numField; ++i)
            {
                //如果存在Field没有值
                if(minField.get(i)>maxField.get(i))
                    intHistograms.put(i ,new IntHistogram(NUM_HIST_BINS, Integer.MIN_VALUE, Integer.MAX_VALUE));
                else
                    intHistograms.put(i, new IntHistogram(NUM_HIST_BINS, minField.get(i), maxField.get(i)));
            }
            iter.rewind();
            //重置iter，将每个tuple中的field存入对应的histogram
            while(iter.hasNext())
            {
                Tuple tup = iter.next();
                for(int i=0; i<numField; ++i)
                {
                    if(tup.getField(i).getType().equals(Type.INT_TYPE))
                    {
                        IntField field = (IntField) tup.getField(i);
                        intHistograms.get(i).addValue(field.getValue());
                    }
                    else
                    {
                        StringField field = (StringField) tup.getField(i);
                        stringHistograms.get(i).addValue(field.getValue());
                    }
                }
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        finally {
            iter.close();
        }
        //计算总共读取的page数
        int pageSize = BufferPool.getPageSize();
        numPages = (numTuples * table.getTupleDesc().getSize())/pageSize + 1;
        //System.out.println(numTuples);
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        return numPages*ioCostPerPage;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        return (int) (numTuples * selectivityFactor);
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        // no need to implement
        return 0.5;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        if(stringHistograms.containsKey(field))
        {
            return stringHistograms.get(field).estimateSelectivity(op, ((StringField) constant).getValue());
        }
        else
            return intHistograms.get(field).estimateSelectivity(op, ((IntField) constant).getValue());
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        return numTuples;
    }

}
