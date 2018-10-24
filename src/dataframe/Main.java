package dataframe;

public class Main {
    public static void main(String[] args) throws Exception{
        /*Class[] arg = new Class[2];
        arg[0]=ValInteger.class;
*/      /*ValDouble hide = new ValDouble(0.0);
        System.out.println(new ValDouble(0.0).eq(hide));*/
       // System.out.println(Integer.valueOf("0.2"));
       // DataFrame dataFrame = new DataFrame("/home/pawelgalka/IdeaProjects/java/src/dataframe/dane.csv",new Class[]{ValInteger.class,ValDouble.class});
        SparseDataFrame sparseDataFrame = new SparseDataFrame("/home/pawelgalka/IdeaProjects/java/src/dataframe/sparse.csv",new Class[]{ValDouble.class,ValDouble.class,ValDouble.class}, true,new ValDouble(0.0));
        sparseDataFrame.print();
        DataFrame dataFrame2 = sparseDataFrame.toDense();
        dataFrame2.print();
      //  System.out.println(.getClass().isAssignableFrom(Value.class));
        /*ValInteger.class type = dataFrame.dataframe.get(1).getType();
        ValInteger valInteger = Class.forName(type.getName());*/
        //dataFrame.add(new Value[]{new ValInteger(1), new ValInteger(11)});
    }
}
