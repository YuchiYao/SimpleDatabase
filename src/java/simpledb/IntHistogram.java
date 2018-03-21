package simpledb;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {


    private int max;
    private int min;
    private int range;
    private int numOfBuckets;
    private int totalNumwidth;
    private int[] buckets;
    private double width;
    private int totalNum;
    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */

    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
        this.min = min;
        this.max = max;
        this.numOfBuckets = buckets;
        this.buckets = new int[numOfBuckets];
        this.range = max - min + 1;
        this.width = (int) Math.ceil((double)range/numOfBuckets);
        this.totalNum = 0;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
        int index = getIndex(v);
        buckets[index]++;
        totalNum++;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {

    	// some code goes here
        if(op == Predicate.Op.EQUALS){
            if(v < min || v > max){
                return 0;
            }
            int index = getIndex(v);
            int height = buckets[index];
            return ((double)height/width)/totalNum;
        } else if(op == Predicate.Op.GREATER_THAN){
            if(v < min){
                return 1;
            }else if(v >= max){
                return 0;
            }else {
                return greaterThanOrEqual(v);
            }
        } else if(op == Predicate.Op.GREATER_THAN_OR_EQ) {
            if(v <= min){
                return 1;
            }else if(v > max){
                return 0;
            }else {
                return greaterThanOrEqual(v);
            }
        } else if(op == Predicate.Op.LESS_THAN) {
            if(v <= min){
                return 0;
            }else if(v > max){
                return 1;
            }else {
                return lessThanOrEqual(v);
            }
        } else if(op == Predicate.Op.LESS_THAN_OR_EQ) {
            if (v < min) {
                return 0;
            } else if (v >= max) {
                return 1;
            } else {
                return lessThanOrEqual(v);
            }
        } else if(op == Predicate.Op.NOT_EQUALS){
            if (v < min || v > max){
                return 1;
            }else{
                return notEqual(v);
            }
        }
        return -1.0;
    }

    private int getIndex(int v){
        int index = (int)((v - min) / width);
        if(index>=numOfBuckets){
            index = numOfBuckets-1;
        }
        return index;
    }



    private double greaterThanOrEqual(int v){
        int index = getIndex(v);
        int height = buckets[index];
        double mid = (v - min - (index) * width);
        double bPart = ((double)width - mid)/width;
        double bFact = (double)height/totalNum;
        double selectOnRight = (double)totalHeightOnRightBuckets(index+1)/totalNum;
        return bPart*bFact+selectOnRight;
    }

    private double lessThanOrEqual(int v){
        int index = getIndex(v);
        int height = buckets[index];
        double mid = v - min - (index) * width + 1;
        double bPart = (double)mid/width;
        double bFact = (double)height/totalNum;
        double selectOnLeft = (double)totalHeightOnLeftBuckets(index-1)/totalNum;
        return bPart*bFact+selectOnLeft;
    }

    private double notEqual(int v){
        int index = getIndex(v);
        int height = buckets[index];
        double bNotPart = (double)height/width;
        return (totalNum-bNotPart)/totalNum;
    }

    private int totalHeightOnRightBuckets(int index){
        int totalHeight = 0;
        while(index < numOfBuckets){
            totalHeight += buckets[index];
            index++;
        }
        return totalHeight;
    }

    private int totalHeightOnLeftBuckets(int index){
        int totalHeight = 0;
        while(index >= 0){
            totalHeight += buckets[index];
            index--;
        }
        return totalHeight;
    }

    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        return null;
    }
}
