package dataframe.tests;

import dataframe.*;
import dataframe.value.ValInteger;
import dataframe.value.ValString;

public class ApplyableTest {
    public static void main(String[] args) throws Exception {
        DataFrame dataFrame = new DataFrame("/home/pawelgalka/IdeaProjects/java/src/dataframe/median.csv",new Class[]{ValString.class, ValInteger.class,ValInteger.class,ValInteger.class});
        //DataFrame grouped = dataFrame.groupby(new String[]{"id"}).apply(new Mediana());
        //grouped.print();

    }
}
