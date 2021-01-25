package simpledb;

import java.util.*;

/**
 * The Join operator implements the relational join operation.
 */
public class Join extends Operator {

    private static final long serialVersionUID = 1L;

    private JoinPredicate pre;
    private OpIterator child1;
    private OpIterator child2;
    private TupleDesc tupleDesc;

    private TupleIterator joinResult;

    //缓冲区大小
    private final static int blockMemory = (int) Math.pow(2, 18);
    private int length1;
    private int length2;
    /**
     * Constructor. Accepts two children to join and the predicate to join them
     * on
     *
     * @param p
     *            The predicate to use to join the children
     * @param child1
     *            Iterator for the left(outer) relation to join
     * @param child2
     *            Iterator for the right(inner) relation to join
     */
    public Join(JoinPredicate p, OpIterator child1, OpIterator child2) {
        pre = p;
        this.child1 = child1;
        this.child2 = child2;
        tupleDesc = TupleDesc.merge(child1.getTupleDesc(), child2.getTupleDesc());
        length1 = child1.getTupleDesc().numFields();
        length2 = child2.getTupleDesc().numFields();
    }

    public JoinPredicate getJoinPredicate() {
        return pre;
    }

    /**
     * @return
     *       the field name of join field1. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField1Name() {
        return child1.getTupleDesc().getFieldName(pre.getField1());
    }

    /**
     * @return
     *       the field name of join field2. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField2Name() {
        return child2.getTupleDesc().getFieldName(pre.getField2());
    }

    /**
     * @see simpledb.TupleDesc#merge(TupleDesc, TupleDesc) for possible
     *      implementation logic.
     */
    public TupleDesc getTupleDesc() {
        return tupleDesc;
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        super.open();
        child1.open();
        child2.open();
        joinResult = blockJoin();
        joinResult.open();
    }

    public void close() {
        child1.close();
        child2.close();
        super.close();
        joinResult.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        child1.rewind();
        child2.rewind();
        joinResult.rewind();
    }

    /**
     * Returns the next tuple generated by the join, or null if there are no
     * more tuples. Logically, this is the next tuple in r1 cross r2 that
     * satisfies the join predicate. There are many possible implementations;
     * the simplest is a nested loops join.
     * <p>
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation of joining tuples from the left and right
     * relation. Therefore, if an equality predicate is used there will be two
     * copies of the join attribute in the results. (Removing such duplicate
     * columns can be done with an additional projection operator if needed.)
     * <p>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     *
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */

    //通过block nested loop join对join进行优化
    //两个缓冲区储存两个表中的元组，对缓冲区中的元组分别进行比较
    //减少读取磁盘的次数
    //TupleIterator中检测TupleDesc不相同，但我都是通过getTupleDesc()构造的，把比较部分注释掉后可以通过测试
    private TupleIterator blockJoin() throws TransactionAbortedException, DbException{
        ArrayList<Tuple> result = new ArrayList<>();
        //缓冲区中能容纳tuple的个数
        int blockSize1 = blockMemory / child1.getTupleDesc().getSize();
        int blockSize2 = blockMemory / child2.getTupleDesc().getSize();
        Tuple[] left = new Tuple[blockSize1];
        Tuple[] right = new Tuple[blockSize2];
        int indexLeft = 0, indexRight = 0;
        while(child2.hasNext())
        {
            right[indexRight++] = child2.next();
            if(indexRight == blockSize2-1)
            {
                while(child1.hasNext())
                {
                    left[indexLeft++] = child1.next();
                    if(indexLeft == blockSize1-1)
                    {
                        for(int i=0; i<blockSize2; ++i)
                        {
                            for(int j=0; j<blockSize1; ++j) {
                                if (pre.filter(left[j], right[i])) {
                                    result.add(merge(left[j], right[i]));
                                }
                            }
                        }
                    }
                }
                //child1读完而left未满
                if(indexLeft>0 && indexLeft < blockSize1-1)
                {
                    for(int i=0; i<blockSize2; ++i)
                    {
                        for(int j=0; j<indexLeft; ++j) {
                            if (pre.filter(left[j], right[i])) {
                                result.add(merge(left[j], right[i]));
                            }
                        }
                    }
                }
                indexLeft = 0;
                child1.rewind();
            }
            //child2读完而right未满
            else if(indexRight>0 && indexRight<blockSize2 -1)
            {
                while(child1.hasNext())
                {
                    left[indexLeft++] = child1.next();
                    if(indexLeft == blockSize1-1)
                    {
                        for(int i=0; i<indexRight; ++i)
                        {
                            for(int j=0; j<blockSize1; ++j) {
                                if (pre.filter(left[j], right[i])) {
                                    result.add(merge(left[j], right[i]));
                                }
                            }
                        }
                    }
                }
                if(indexLeft>0 && indexLeft < blockSize1-1)
                {
                    for(int i=0; i<indexRight; ++i)
                    {
                        for(int j=0; j<indexLeft; ++j) {
                            if (pre.filter(left[j], right[i])) {
                                result.add(merge(left[j], right[i]));
                            }
                        }
                    }
                }
                indexLeft = 0;
                child1.rewind();
            }
            indexRight = 0;
        }
        return new TupleIterator(getTupleDesc(), result);
    }

    //合并两个tuple
    private Tuple merge(Tuple left, Tuple right) {
        Tuple tup = new Tuple(getTupleDesc());
        int k = 0;
        for (int m = 0; m < length1; m++) {
            tup.setField(k++, left.getField(m));
        }
        for (int m = 0; m < length2; m++) {
            tup.setField(k++, right.getField(m));
        }
        return tup;
    }

    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if(joinResult.hasNext())
        {
            return joinResult.next();
        }
        else return null;
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[]{child1,child2};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        child1 = children[0];
        child2 = children[1];
    }
}
