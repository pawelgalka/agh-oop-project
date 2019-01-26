package dataframe.tests;

import dataframe.*;
import dataframe.value.*;

public class GroupBySingleTest {
    public static void main(String[] args) throws Exception {
        long t = System.currentTimeMillis();
        DataFrameThreads dataFrameThreads = new DataFrameThreads("/home/pawelgalka/csv/groupby.csv", new Class[]{StringHolder.class, DateTimeHolder.class,  DoubleHolder.class, FloatHolder.class});
//        DataFrame dataFrame = new DataFrame("/home/pawelgalka/csv/large_groupby_3.csv", new Class[]{IntHolder.class, DateTimeHolder.class, StringHolder.class, DoubleHolder.class, FloatHolder.class});
        System.out.println(System.currentTimeMillis() - t);

        DataFrame groupByDataFrame = dataFrameThreads.groupby(new String[]{"id"}).mean();
        System.out.println(groupByDataFrame);
//        System.out.println(groupByDataFrame.getGroupDataFrameList().size());

//   /     t = System.currentTimeMillis();
//        System.out.println(System.currentTimeMillis());
//        groupByDataFrame.var();
//        System.out.println(System.currentTimeMillis() - t);
//        t = System.currentTimeMillis();
//        System.out.println(System.currentTimeMillis());
//        groupByDataFrame.varParallel();
//        System.out.println(System.currentTimeMillis() - t);
    }
}