package simpledb;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    private OpIterator child;
    private final int afield;
    private final int gfield;
    private final Aggregator.Op aop;
    private final Aggregator aggregator;
    private final TupleDesc tupleDesc;
    private OpIterator aggregatorIter;
    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     * 
     * 
     * @param child
     *            The OpIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
        this.child = child;
        this.afield = afield;
        this.gfield = gfield;
        this.aop = aop;
        Type atype = child.getTupleDesc().getFieldType(afield);
        String aname = child.getTupleDesc().getFieldName(afield);
        if(gfield == Aggregator.NO_GROUPING)
        {
            tupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{aname});
            if(atype == Type.STRING_TYPE)
                aggregator = new StringAggregator(gfield, null, afield, aop);
            else
                aggregator = new IntegerAggregator(gfield, null, afield, aop);
        }
        else
        {
            Type gtype = child.getTupleDesc().getFieldType(gfield);
            String gname = child.getTupleDesc().getFieldName(gfield);
            tupleDesc = new TupleDesc(new Type[]{gtype, Type.INT_TYPE}, new String[]{gname, aname});
            if(atype == Type.STRING_TYPE)
                aggregator = new StringAggregator(gfield, gtype, afield, aop);
            else
                aggregator = new IntegerAggregator(gfield, gtype, afield, aop);
        }
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
	    if(gfield == -1)
	        return Aggregator.NO_GROUPING;
	    else
	        return gfield;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     *         null;
     * */
    public String groupFieldName() {
        if(gfield == -1)
            return null;
        else
            return child.getTupleDesc().getFieldName(gfield);
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
	    return afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
        return child.getTupleDesc().getFieldName(afield);
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
	    return aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
	return aop.toString();
    }

    //在运算符启用时就进行计算，合并操作是在Aggregator中进行的
    public void open() throws NoSuchElementException, DbException,
	    TransactionAbortedException {
        super.open();
        child.open();
        while (child.hasNext())
        {
            this.aggregator.mergeTupleIntoGroup(child.next());
        }
        aggregatorIter = this.aggregator.iterator();
        aggregatorIter.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if(aggregatorIter.hasNext())
        {
            return aggregatorIter.next();
        }
        else return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        /* todo 直接设为关闭再重新打开可以通过rewind测试，但其他测试通不过，原因有待思考
        close();
        open();
         */
        aggregatorIter.rewind();
        child.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * 
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
	    return tupleDesc;
    }

    public void close() {
        child.close();
        super.close();
        aggregatorIter.close();
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[]{child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        child = children[0];
    }
    
}
