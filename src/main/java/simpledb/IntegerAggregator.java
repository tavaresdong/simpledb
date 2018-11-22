package simpledb;

import javax.print.DocFlavor;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

import static simpledb.Aggregator.Op.MAX;
import static simpledb.Aggregator.Op.MIN;
import static simpledb.Aggregator.Op.SUM;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbField;
    private Type gbFieldType;
    private int aggField;
    private Op what;

    private int count;
    private Map groupAggregates;
    private Map groupAggregatesCount;
    private Integer noGroupAggregate;

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
        this.gbField = gbfield;
        this.gbFieldType = gbfieldtype;
        this.aggField = afield;
        this.what = what;
        this.count = 0;

        this.noGroupAggregate = 0;
        if (this.what == MAX) {
            this.noGroupAggregate = Integer.MIN_VALUE;
        } else if (this.what == MIN) {
            this.noGroupAggregate = Integer.MAX_VALUE;
        }

        if (this.gbFieldType != null) {
            if (this.gbFieldType == Type.INT_TYPE) {
                this.groupAggregates = new HashMap<Integer, Integer>();
                this.groupAggregatesCount = new HashMap<Integer, Integer>();
            } else {
                this.groupAggregates = new HashMap<String, Integer>();
                this.groupAggregatesCount = new HashMap<Integer, Integer>();
            }
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        int value = ((IntField) tup.getField(this.aggField)).getValue();

        this.count++;
        if (this.gbField == Aggregator.NO_GROUPING) {
            switch (this.what) {
                case SUM:
                case AVG:
                    this.noGroupAggregate += value;
                    break;
                case MAX:
                    this.noGroupAggregate = Math.max(this.noGroupAggregate, value);
                    break;
                case MIN:
                    this.noGroupAggregate = Math.min(this.noGroupAggregate, value);
                    break;
                case COUNT:
                    this.noGroupAggregate += 1;
                    break;
            }
        } else {
            String sKey = null;
            Integer iKey = 0;
            Field groupByFieldValue = tup.getField(this.gbField);
            if (this.gbFieldType == Type.INT_TYPE) {
                iKey = ((IntField) groupByFieldValue).getValue();
                Map<Integer, Integer> group = (Map<Integer, Integer>) this.groupAggregates;
                Map<Integer, Integer> groupCount = (Map<Integer, Integer>) this.groupAggregatesCount;

                switch (this.what) {
                    case SUM:
                    case AVG:
                        if (group.containsKey(iKey)) {
                            group.put(iKey, group.get(iKey) + value);
                            groupCount.put(iKey, groupCount.get(iKey) + 1);

                        } else {
                            group.put(iKey, value);
                            groupCount.put(iKey, 1);
                        }
                        break;
                    case MAX:
                        if (group.containsKey(iKey)) {
                            group.put(iKey, Math.max(group.get(iKey), value));
                            groupCount.put(iKey, groupCount.get(iKey) + 1);
                        } else {
                            group.put(iKey, value);
                            groupCount.put(iKey, 1);
                        }
                        break;
                    case MIN:
                        if (group.containsKey(iKey)) {
                            group.put(iKey, Math.min(group.get(iKey), value));
                            groupCount.put(iKey, groupCount.get(iKey) + 1);
                        } else {
                            group.put(iKey, value);
                            groupCount.put(iKey, 1);
                        }
                        break;
                    case COUNT:
                        if (group.containsKey(iKey)) {
                            group.put(iKey, group.get((iKey) + 1));
                            groupCount.put(iKey, groupCount.get(iKey) + 1);
                        } else {
                            group.put(iKey, 1);
                            groupCount.put(iKey, 1);
                        }
                        break;
                }
            } else if (this.gbFieldType == Type.STRING_TYPE){
                sKey = ((StringField) groupByFieldValue).getValue();
                Map<String, Integer> group = (Map<String, Integer>) this.groupAggregates;
                Map<String, Integer> groupCount = (Map<String, Integer>) this.groupAggregatesCount;

                switch (this.what) {
                    case SUM:
                    case AVG:
                        if (group.containsKey(sKey)) {
                            group.put(sKey, group.get(sKey) + value);
                            groupCount.put(sKey, groupCount.get(sKey) + 1);
                        } else {
                            group.put(sKey, value);
                            groupCount.put(sKey, 1);
                        }
                        break;
                    case MAX:
                        if (group.containsKey(sKey)) {
                            group.put(sKey, Math.max(group.get(sKey), value));
                            groupCount.put(sKey, groupCount.get(sKey) + 1);
                        } else {
                            group.put(sKey, value);
                            groupCount.put(sKey, 1);
                        }
                        break;
                    case MIN:
                        if (group.containsKey(sKey)) {
                            group.put(sKey, Math.min(group.get(sKey), value));
                            groupCount.put(sKey, groupCount.get(sKey) + 1);
                        } else {
                            group.put(sKey, value);
                            groupCount.put(sKey, 1);
                        }
                        break;
                    case COUNT:
                        if (group.containsKey(sKey)) {
                            group.put(sKey, group.get((sKey) + 1));
                            groupCount.put(sKey, groupCount.get(sKey) + 1);
                        } else {
                            group.put(sKey, 1);
                            groupCount.put(sKey, 1);
                        }
                        break;
                }
            }
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
        return new IntegerAggIterator(this.gbField,
                this.gbFieldType, this.what, this.count,
                this.groupAggregates, this.groupAggregatesCount,
                this.noGroupAggregate);
    }

    private class IntegerAggIterator implements OpIterator {

        private Type gbFieldType;
        private Op what;
        private int gbField;

        private int count;
        private Map groupAggregates;
        private Map getGroupAggregatesCount;
        private Integer noGroupAggregate;
        private int curIdx;
        private TupleDesc desc;
        private Iterator groupIter;

        public IntegerAggIterator(int gbField, Type gbFieldType, Op w,
                                  int c, Map group, Map groupCount, Integer noGroup) {
            this.gbFieldType = gbFieldType;
            this.what = w;
            this.count = c;
            this.groupAggregates = group;
            this.getGroupAggregatesCount = groupCount;
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
                desc = new TupleDesc(t, n);
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

        private void setAggregateField(int idx, Tuple t, int aggValue, int count) {
            switch (this.what) {
                case MAX:
                case MIN:
                case COUNT:
                case SUM:
                    t.setField(idx, new IntField(aggValue));
                    break;
                case AVG:
                    t.setField(idx, new IntField(aggValue / count));
                    break;
            }
        }

        private void setField(Tuple t) {
            if (this.gbField == Aggregator.NO_GROUPING) {
                setAggregateField(0, t, this.noGroupAggregate, this.count);
            }
            else if (this.gbFieldType == Type.INT_TYPE) {
                Map.Entry<Integer, Integer> entry = (Map.Entry<Integer, Integer>) this.groupIter.next();
                t.setField(0, new IntField(entry.getKey()));

                int groupCount = ((Map<Integer, Integer>) this.getGroupAggregatesCount).get(entry.getKey());
                setAggregateField(1, t, entry.getValue(), groupCount);
            } else {
                Map.Entry<String, Integer> entry = (Map.Entry<String, Integer>) this.groupIter.next();
                t.setField(0, new StringField(entry.getKey(), 5000));

                int groupCount = ((Map<String, Integer>) this.getGroupAggregatesCount).get(entry.getKey());
                setAggregateField(1, t, entry.getValue(), groupCount);
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
