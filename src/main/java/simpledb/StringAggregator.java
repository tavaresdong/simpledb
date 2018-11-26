package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

import static simpledb.Aggregator.Op.MAX;
import static simpledb.Aggregator.Op.MIN;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbFieldIdx;
    private Type gbFieldType;
    private int aggField;
    private Op what;

    private Map groupAggregates;

    private Integer noGroupAggregate;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbFieldIdx = gbfield;
        this.gbFieldType = gbfieldtype;
        this.aggField = afield;
        this.what = what;

        if (what != Op.COUNT) {
            throw new IllegalArgumentException("StringAggregator only supports COUNT");
        }

        this.noGroupAggregate = 0;
        if (this.gbFieldType != null) {
            if (this.gbFieldType == Type.INT_TYPE) {
                this.groupAggregates = new HashMap<Integer, Integer>();
            } else {
                this.groupAggregates = new HashMap<String, Integer>();
            }
        }
    }

    private <T> void calculateAggregation(Tuple tup, T key) {
        Map<T, Integer> group = (Map<T, Integer>) this.groupAggregates;

        if (group.containsKey(key)) {
            group.put(key, group.get(key) + 1);
        } else {
            group.put(key, 1);
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        String value = ((StringField) tup.getField(this.aggField)).getValue();

        if (this.gbFieldIdx == Aggregator.NO_GROUPING) {
            this.noGroupAggregate += 1;
        } else {
            Field gbField = tup.getField(this.gbFieldIdx);
            if (this.gbFieldType == Type.INT_TYPE) {
                Integer key = ((IntField) gbField).getValue();
                calculateAggregation(tup, key);
            } else if (this.gbFieldType == Type.STRING_TYPE){
                String key = ((StringField) gbField).getValue();
                calculateAggregation(tup, key);
            }
        }
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
        return new StringAggIterator(this.gbFieldIdx, this.gbFieldType, this.groupAggregates, this.noGroupAggregate);
    }

    private class StringAggIterator implements OpIterator {

        private Type gbFieldType;
        private int gbField;

        private Map groupAggregates;
        private Integer noGroupAggregate;
        private int curIdx;
        private TupleDesc desc;
        private Iterator groupIter;

        public StringAggIterator(int gbField, Type gbFieldType, Map group, Integer noGroup) {
            this.gbFieldType = gbFieldType;
            this.groupAggregates = group;
            this.noGroupAggregate = noGroup;
            this.gbField = gbField;
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            if (this.gbField == Aggregator.NO_GROUPING) {
                Type[] t = new Type[1];
                t[0] = Type.INT_TYPE;
                String[] n = new String[1];
                n[0] = "aggregateVal";
                desc = new TupleDesc(t, n);
                this.curIdx = 0;
            } else {
                Type[] t = new Type[2];
                t[0] = this.gbFieldType;
                t[1] = Type.INT_TYPE;
                String[] n = new String[2];
                n[0] = "groupVal";
                n[1] = "aggregateVal";
                this.desc = new TupleDesc(t, n);
                this.groupIter = this.groupAggregates.entrySet().iterator();
            }
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (this.gbField == Aggregator.NO_GROUPING) {
                return this.curIdx == 0;
            } else {
                return this.groupIter.hasNext();
            }
        }

        private void setAggregateField(int idx, Tuple t, int aggValue) {
            t.setField(idx, new IntField(aggValue));
        }

        private void setField(Tuple t) {
            if (this.gbField == Aggregator.NO_GROUPING) {
                setAggregateField(0, t, this.noGroupAggregate);
            }
            else if (this.gbFieldType == Type.INT_TYPE) {
                Map.Entry<Integer, Integer> entry = (Map.Entry<Integer, Integer>) this.groupIter.next();
                t.setField(0, new IntField(entry.getKey()));
                setAggregateField(1, t, entry.getValue());
            } else {
                Map.Entry<String, Integer> entry = (Map.Entry<String, Integer>) this.groupIter.next();
                t.setField(0, new StringField(entry.getKey(), 5000));
                setAggregateField(1, t, entry.getValue());
            }
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            Tuple t = new Tuple(this.desc);
            setField(t);
            this.curIdx++;
            return t;
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            if (this.gbField == Aggregator.NO_GROUPING) {
                this.curIdx = 0;
            } else {
                this.groupIter = this.groupAggregates.entrySet().iterator();
            }
        }

        @Override
        public TupleDesc getTupleDesc() {
            return this.desc;
        }

        @Override
        public void close() {
        }
    }
}
