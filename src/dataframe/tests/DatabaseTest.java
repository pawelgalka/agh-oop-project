package dataframe.tests;

import DataFrameDB.DataFrameDB;
import dataframe.*;
import dataframe.value.ValDateTime;
import dataframe.value.ValDouble;
import dataframe.value.ValString;

public class DatabaseTest {
    public static void main(String[] args) {
        DataFrameDB dataFrameDB = new DataFrameDB("books");

        dataFrameDB.open("books");
        DataFrame df = dataFrameDB.toDF();
        //dataFrameDB.print();
       // df.print();

        //DataFrameDB dataFrameDB1 = new DataFrameDB(df,"abcdef");
//        DataFrame df1 = dataFrameDB.query("SELECT year FROM books");
//        DataFrameDB dataFrameDB2 = new DataFrameDB("/home/pawelgalka/csv/dane.csv",new Class[]{ValString.class, ValDateTime.class, ValDouble.class,ValDouble.class},true,"dataframe1");
        DataFrameDB dataFrameDB2 = new DataFrameDB("dataframe1");
//        DataFrame dataFrame = dataFrameDB2.query("SELECT id,min(date) from dataframe1 GROUP BY id");
        DataFrame dataFrame = dataFrameDB2.min("total","val");
        DataFrame dataFrame1 = dataFrameDB2.max("total");
        DataFrame.GroupByDataFrame groupByDataFrame = dataFrameDB2.groupBy("id", "date");
        groupByDataFrame.getGroupDataFrameList().get(0).print();
//        dataFrame.print();
//        dataFrame1.print();
        dataFrameDB.close();
    }

}
