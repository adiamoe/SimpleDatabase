package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private final int gfield;
    private final int afield;
    private Type gfieldtype;
    private Op aop;
    private TupleDesc tupleDesc;
    //根据不同字段分组对应的结果
    private HashMap<Field, Integer> Fie2Agg;
    //根据不同字段分组对应的数量,用于计算平均值
    private HashMap<Field, Integer[]> Fie2Num;
    private final Field dummy;
    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        gfield = gbfield;
        gfieldtype = gbfieldtype;
        this.afield = afield;
        aop = what;
        Fie2Agg = new HashMap<>();
        Fie2Num = new HashMap<>();
        if(gfield == Aggregator.NO_GROUPING)
            tupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE});
        else
            tupleDesc = new TupleDesc(new Type[]{gfieldtype, Type.INT_TYPE});
        dummy = null;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        if(tup.getTupleDesc().getFieldType(afield) != Type.INT_TYPE)
            throw new IllegalArgumentException("Not a valid tuple!");
        //dummy为空值，Hashmap可以用空值作为key
        Field key = gfield == Aggregator.NO_GROUPING? dummy :tup.getField(gfield);
        Field aggField = tup.getField(afield);
        int value = ((IntField) aggField).getValue();
        //如果Fie2Agg中没有对应的字符，就添加该字符对应的值。如果有就取出对应的值
        switch (aop)
        {
            case MIN:
                Fie2Agg.put(key, Fie2Agg.containsKey(key)? Math.min(Fie2Agg.get(key), value):value);
                break;
            case MAX:
                Fie2Agg.put(key, Fie2Agg.containsKey(key)? Math.max(Fie2Agg.get(key), value):value);
                break;
            case SUM:
                Fie2Agg.put(key, Fie2Agg.containsKey(key)? Fie2Agg.get(key)+ value:value);
                break;
            case COUNT:
                Fie2Agg.put(key, Fie2Agg.containsKey(key)? Fie2Agg.get(key)+1:1);
                break;
            case AVG:
                //1-19:计算平均值，需要计算字符出现的次数和对应数字的总和
                //对于每次输入，都计算现有所有数字的平均值

                //1-21更正:一开始我通过每次计算平均值，在通过平均值和总数求总和通不过测试，
                //类型转换会导致数据发生变化，需要储存count和sum两个变量
                if(Fie2Agg.containsKey(key))
                {
                    int count = Fie2Num.get(key)[0];
                    int sum = Fie2Num.get(key)[1];
                    count++;
                    sum += value;
                    Integer[] nums = new Integer[]{count, sum};
                    int avg = sum/count;
                    Fie2Agg.put(key, avg);
                    Fie2Num.put(key, nums);
                }
                else
                {
                    Fie2Agg.put(key, value);
                    Integer[] nums = new Integer[]{1, value};
                    Fie2Num.put(key, nums);
                }
                break;
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        ArrayList<Tuple> tuples = new ArrayList<>();
        for (Map.Entry<Field, Integer> entry : Fie2Agg.entrySet()) {
            Tuple t = new Tuple(tupleDesc);
            if (gfield == Aggregator.NO_GROUPING) {
                t.setField(0, new IntField(entry.getValue()));
            } else {
                t.setField(0, entry.getKey());
                t.setField(1, new IntField(entry.getValue()));
            }
            tuples.add(t);
        }
        //直接返回TupleIterator，避免重复覆写
        return new TupleIterator(tupleDesc, tuples);
    }
}
