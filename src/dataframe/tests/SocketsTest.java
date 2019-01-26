package dataframe.tests;

import dataframe.DataFrameClient;
import dataframe.value.DateTimeHolder;
import dataframe.value.DoubleHolder;
import dataframe.value.StringHolder;

public class SocketsTest {
    public static void main(String[] args) throws Exception{
        DataFrameClient df2 = new DataFrameClient("/home/pawelgalka/csv/dane1.csv",new Class[]{StringHolder.class, DateTimeHolder.class, DoubleHolder.class, DoubleHolder.class});
       /* DataFrameClient.GroupByDataFrameSockets group = */df2.groupby("id").max();
//        df2.groupby("date").max();
        //group.max();

    }

}
