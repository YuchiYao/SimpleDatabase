package simpledb;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfieldIndex;
    private Type gbfieldtype;
    private Op what;
    private TupleDesc tupleDesc;
    private HashMap<Field,Integer> counts;
    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        if(what != Op.COUNT){
            throw new IllegalArgumentException("Only support 'COUNT' operator for string aggregator");
        }
        this.gbfieldIndex = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.what = what;
        counts = new HashMap<>();
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
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
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
        if (counts.containsKey(gbField)) {
            counts.put(gbField, counts.get(gbField) + 1);
        } else {
            counts.put(gbField, 1);
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
        // some code goes here
        ArrayList<Tuple> tuples = new ArrayList<>();

        if(gbfieldIndex==NO_GROUPING){
            Tuple tuple = new Tuple(tupleDesc);
            tuple.setField(0,new IntField(counts.get(null)));
            tuples.add(tuple);
        }else{
            for(Field key: counts.keySet()){
                Tuple tuple = new Tuple(tupleDesc);
                tuple.setField(0,key);
                tuple.setField(1,new IntField(counts.get(key)));
                tuples.add(tuple);
            }
        }
        return new TupleIterator(tupleDesc,tuples);
    }

    public TupleDesc getTupleDesc(){
        return tupleDesc;
    }
}
