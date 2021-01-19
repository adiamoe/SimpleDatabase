package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private final int gfield;
    private final Type gfieldtype;
    private final int afield;
    private final Op aop;
    private HashMap<Field, Integer> Fie2Agg;
    private TupleDesc tupleDesc;
    private Field dummy;
    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        gfield = gbfield;
        gfieldtype = gbfieldtype;
        this.afield = afield;
        aop = what;
        if(gfield == Aggregator.NO_GROUPING)
            tupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE});
        else
            tupleDesc = new TupleDesc(new Type[]{gfieldtype, Type.INT_TYPE});
        dummy = null;
        Fie2Agg = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        if(aop != Op.COUNT)
            throw new IllegalArgumentException("only for count");
        if(tup.getTupleDesc().getFieldType(afield)!= Type.STRING_TYPE)
            throw new IllegalArgumentException("only for string");
        Field key = gfield == Aggregator.NO_GROUPING? dummy: tup.getField(gfield);
        Fie2Agg.put(key, Fie2Agg.containsKey(key)?Fie2Agg.get(key)+1:1);
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        ArrayList<Tuple> tuples = new ArrayList<>();
        for (Map.Entry<Field, Integer> fieldIntegerEntry : Fie2Agg.entrySet()) {
            Tuple t = new Tuple(tupleDesc);
            if (gfield == Aggregator.NO_GROUPING)
                t.setField(0, new IntField(fieldIntegerEntry.getValue()));
            else {
                t.setField(0, fieldIntegerEntry.getKey());
                t.setField(1, new IntField(fieldIntegerEntry.getValue()));
            }
            tuples.add(t);
        }
        return new TupleIterator(tupleDesc, tuples);
    }

}
