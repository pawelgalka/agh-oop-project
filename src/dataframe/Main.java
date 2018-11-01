package dataframe;

import java.sql.Time;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedList;

public class Main {
    public static void main(String[] args) throws Exception{
        /*Class[] arg = new Class[2];
        arg[0]=ValInteger.class;
*/      /*ValDouble hide = new ValDouble(0.0);
        System.out.println(new ValDouble(0.0).eq(hide));*/
       // System.out.println(Integer.valueOf("0.2"));
        //System.out.println(Math.pow((-0.4524634484707701-0.000313),2));
        long time = System.currentTimeMillis();
        DataFrame dataFrame = new DataFrame("/home/pawelgalka/IdeaProjects/java/src/dataframe/groubymulti.csv",new Class[]{ValString.class,ValDateTime.class,ValDouble.class,ValDouble.class});
//        DataFrame dataFrame = new DataFrame("/home/pawelgalka/IdeaProjects/java/src/dataframe/data.csv",new Class[]{ValDouble.class,ValDouble.class,ValDouble.class});
        System.out.println(System.currentTimeMillis()-time);
//        System.out.println(dataFrame.size());
        //DataFrame groupby = dataFrame.groupby(new String[]{"id"}).max();
        //DataFrame groupby1 = dataFrame.groupby(new String[]{"id"}).min();
       /* DataFrame groupby2 = dataFrame.groupby(new String[]{"id"}).mean();


        DataFrame groupby3 = dataFrame.groupby(new String[]{"id"}).sum();

        DataFrame groupby4 = dataFrame.groupby(new String[]{"id"}).std();

        DataFrame groupby5 = dataFrame.groupby(new String[]{"id"}).var();

        //System.out.println(linkedList.size());
        System.out.println("-----max-----");
        /*for (DataFrame df:groupby.groupDataFrameList){
            df.print();
        }*/
       //dataFrame.print();
//        groupby.print();
//        System.out.println("-----min-----");

//        groupby1.print();
/*        System.out.println("-----mean-----");

        groupby2.print();
       System.out.println("-----sum-----");

        groupby3.print();
        System.out.println("------std----");
        groupby4.print();

        System.out.println("------var----");
        groupby5.print();*/
        DataFrame groupbymax = dataFrame.groupby(new String[]{"id","date"}).max();
        groupbymax.print();
        System.out.println();
        System.out.println();
        System.out.println();
        DataFrame groupbymin = dataFrame.groupby(new String[]{"id","date"}).min();
        groupbymin.print();
        System.out.println();
        System.out.println();
        System.out.println();
        DataFrame groupbysum = dataFrame.groupby(new String[]{"id","date"}).sum();
        groupbysum.print();
        System.out.println();
        System.out.println();
        System.out.println();
        System.out.println();
        DataFrame groupbymean = dataFrame.groupby(new String[]{"id","date"}).mean();
        groupbymean.print();
        System.out.println();
        System.out.println();
        System.out.println();
        DataFrame groupbystd = dataFrame.groupby(new String[]{"id","date"}).std();
        groupbystd.print();
        System.out.println();
        System.out.println();
        System.out.println();
        DataFrame groupbyvar = dataFrame.groupby(new String[]{"id","date"}).var();
        groupbyvar.print();


        System.out.println();
        /*DataFrame.GroupByDataFrame df = dataFrame.groupby(new String[]{"id","date"});
        df.groupDataFrameList.get(0).print();*/
     /*ArrayList<Value> values = new ArrayList<>();
        Value a = ValDateTime.getInstance().create("2258-11-01");
        Value b = ValDateTime.getInstance().create("2258-10-19");
        Value c = ValDateTime.getInstance().create("2258-10-31");
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        Date date = dateFormat.parse("2258-11-01");
        Date date1 =  dateFormat.parse("2258-10-31");
        System.out.println(date.after(date1));*/
        /*System.out.println(((ValDateTime) a).getValue().before(((ValDateTime) c).getValue()));
        values.add(a);
        values.add(b);
        values.add(c);
        System.out.println(Collections.max(values));
        //long startTime = System.currentTimeMillis();
        SparseDataFrame dataFrame = new SparseDataFrame("/home/pawelgalka/IdeaProjects/java/src/dataframe/sparse.csv",new Class[]{ValDouble.class,ValDouble.class,ValDouble.class},new ValDouble(0.0));
        long s1 = System.currentTimeMillis();
        dataFrame.print();
        dataFrame.toDense().print();
        long stopTime = System.currentTimeMillis();
        System.out.println(s1-startTime);
        System.out.println(stopTime - startTime);
       /* DataFrame dataFrame2 = sparseDataFrame.toDense();
        dataFrame2.print();*/
      //  System.out.println(.getClass().isAssignableFrom(Value.class));
        /*ValInteger.class type = dataFrame.dataframe.get(1).getType();
        ValInteger valInteger = Class.forName(type.getName());*/
        //dataFrame.add(new Value[]{new ValInteger(1), new ValInteger(11)});
        //DataFrame dataFrame = new DataFrame()
    }
}
