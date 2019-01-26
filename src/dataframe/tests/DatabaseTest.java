package dataframe.tests;

import DataFrameDB.DataFrameDB;
import dataframe.*;
import dataframe.value.*;

public class DatabaseTest {
    public static void main(String[] args) throws Exception {

        DataFrameDB dataFrameDB2 = new DataFrameDB("/home/pawelgalka/csv/dane1.csv", new Class[]{StringHolder.class, DateTimeHolder.class, DoubleHolder.class, FloatHolder.class},true,"large");
        System.out.println(dataFrameDB2);
//        DataFrame dataFrame = dataFrameDB2.iloc(2,3);
//        System.out.println(dataFrame);
        DataFrameDB.GroupByDataFrameDf grouped = dataFrameDB2.groupby("id");
//        DataFrame max = grouped.max();
//        System.out.println(max);

    }

}
