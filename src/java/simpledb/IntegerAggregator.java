package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

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

    private int gbfieldIndex;
    private Type gbfieldtype;
    private int afieldIndex;
    private Op what;
    private HashMap<Field,Integer> groups;
    private HashMap<Field,Integer> counts;
    private HashMap<Field,Integer> sums;
    private TupleDesc tupleDesc;

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbfieldIndex = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afieldIndex = afield;
        this.what = what;
        groups = new HashMap<>();
        counts = new HashMap<>();
        sums = new HashMap<>();
        if(gbfieldIndex==NO_GROUPING) {
            tupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE},
                    new String[]{"aggregateVal"});
        } else {
            Type aggType;
            tupleDesc = new TupleDesc(new Type[]{gbfieldtype,Type.INT_TYPE},
                    new String[]{"groupVal","aggregateVal"});
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
        // some code goes here
        Field gbField;
        //check if there is no grouping. if no grouping, set gbfield as null
        if(gbfieldIndex==NO_GROUPING){
            gbField = null;
        } else{
            gbField = tup.getField(gbfieldIndex);
        }

        Field afield = tup.getField(afieldIndex);
        if(afield.getType()!=Type.INT_TYPE){
            throw new UnsupportedOperationException("aggregate field type should be integer");
        }
        IntField intField = (IntField) afield;
        Integer value = intField.getValue();

        switch (what) {
            case MIN:
                if (groups.containsKey(gbField)) {
                    groups.put(gbField, Integer.min(value, groups.get(gbField)));
                } else {
                    groups.put(gbField, value);
                }
                break;
            case MAX:
                if (groups.containsKey(gbField)) {
                    groups.put(gbField, Integer.max(value, groups.get(gbField)));
                } else {
                    groups.put(gbField, value);
                }
                break;
            case COUNT:
                if (groups.containsKey(gbField)) {
                    groups.put(gbField, groups.get(gbField) + 1);
                } else {
                    groups.put(gbField, 1);
                }
                break;
            case SUM:
                if (groups.containsKey(gbField)) {
                    groups.put(gbField, groups.get(gbField) + value);
                } else {
                    groups.put(gbField, value);
                }
                break;
            case AVG:
                if (groups.containsKey(gbField)) {
                    int newCount = counts.get(gbField) + 1;
                    counts.put(gbField, newCount);
                    int newSum = sums.get(gbField) + value;
                    sums.put(gbField, newSum);
                    groups.put(gbField, newSum / newCount);
                } else {
                    counts.put(gbField, 1);
                    sums.put(gbField, value);
                    groups.put(gbField, value);
                }
                break;
            default:
                throw new UnsupportedOperationException("Unsupported Operator");
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
        // some code goes here
        ArrayList<Tuple> tuples = new ArrayList<>();

        if(gbfieldIndex==NO_GROUPING){
            Tuple tuple = new Tuple(tupleDesc);
            tuple.setField(0,new IntField(groups.get(null)));
            tuples.add(tuple);
        }else{
            for(Field key: groups.keySet()){
                Tuple tuple = new Tuple(tupleDesc);
                tuple.setField(0,key);
                tuple.setField(1,new IntField(groups.get(key)));
                tuples.add(tuple);
            }
        }
        return new TupleIterator(tupleDesc,tuples);
    }

    public TupleDesc getTupleDesc(){
        return tupleDesc;
    }

}
