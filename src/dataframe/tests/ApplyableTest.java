package dataframe.tests;

import dataframe.*;
import dataframe.value.IntHolder;
import dataframe.value.StringHolder;

public class ApplyableTest {
    public static void main(String[] args) throws Exception {
        DataFrame dataFrame = new DataFrame("/home/pawelgalka/IdeaProjects/java/src/dataframe/median.csv",new Class[]{StringHolder.class, IntHolder.class, IntHolder.class, IntHolder.class});
        //DataFrame grouped = dataFrame.groupby(new String[]{"id"}).apply(new Mediana());
        //grouped.print();

    }
}
