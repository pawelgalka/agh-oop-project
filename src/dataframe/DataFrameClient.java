package dataframe;

import dataframe.exceptions.CustomException;
import dataframe.groupby.Applyable;
import dataframe.groupby.GroupBy;
import dataframe.value.*;

import java.io.*;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DataFrameClient extends DataFrame implements Serializable{
    public DataFrameClient(String path, Class[] classes) throws IOException{
        super(path,classes);
        dataFrameToPass = new DataFrame(path,classes);
        initialize();


    }
    DataFrame dataFrameToPass;
    DataFrame.GroupByDataFrame grouped;
    Socket echoSocket = null;
    PrintWriter out = null;
    BufferedReader in = null;
    ObjectOutputStream outToServer = null;
    ObjectInputStream inFromServer = null;


    public GroupByDataFrameSockets groupby(String... colnames) throws Exception {
        /*HashMap<List<Value>, DataFrame> map = new HashMap<>(colnames.length);
        List<Column> columns1 = Arrays.stream(colnames).map(dataFrameToPass::get).collect(Collectors.toList());
        ArrayList<Integer> indexes = dataFrameToPass.GetIndexesOfColumns(colnames);
        for (int i = 0; i < dataFrameToPass.size(); i++) {
            List<Value> values = new ArrayList<>(columns1.size());
            for (Column column : columns1) {
                values.add(column.getArrayList().get(i));
            }

            if (!map.containsKey(values)) {
                map.put(values, dataFrameToPass.iloc(i));
            } else {
                map.get(values).add(dataFrameToPass.getRecord(i));
            }
        }
        grouped =  new DataFrame.GroupByDataFrameThreads(new LinkedList<DataFrame>(map.values()), dataFrameToPass.columns, dataFrameToPass.types, indexes);
        return new GroupByDataFrameSockets(new LinkedList<DataFrame>(map.values()), dataFrameToPass.columns, dataFrameToPass.types, indexes);*/
        grouped = dataFrameToPass.groupby(colnames);
        return new GroupByDataFrameSockets(grouped);
    }

    private void initialize() throws IOException{
        try {
        echoSocket = new Socket("localhost", 9000); //1000 - port proxy
        outToServer = new ObjectOutputStream(echoSocket.getOutputStream());
        inFromServer = new ObjectInputStream(echoSocket.getInputStream());

    } catch (
    UnknownHostException e) {
        System.err.println("Don't know about host: localhost.");
        System.exit(1);
    } catch (
    IOException e) {
        System.err.println("Couldn't get I/O for "
                + "the connection to: localhost.");
        System.exit(1);
    }
       // System.out.println("b");
    }
    BufferedReader stdIn = null;

    public class GroupByDataFrameSockets extends DataFrame.GroupByDataFrame implements Serializable {
        public GroupByDataFrameSockets(DataFrame.GroupByDataFrame group) {
            super(group.groupDataFrameList, group.columns, group.types, group.groupedCols);
        }

        public GroupByDataFrameSockets(String[] cols, Class[] types, ArrayList<Integer> groupedCols) {
            super(cols, types, groupedCols);
        }

        @Override
        public DataFrame max() {
            try {
                return message("max");
            } catch (Exception e) {
                e.printStackTrace();
            }
            return null;
        }

        @Override
        public DataFrame min() {
            try {
                return message("min");
            } catch (Exception e) {
                e.printStackTrace();
            }
            return null;
        }

        @Override
        public DataFrame mean() {
            try {
                return message("mean");
            } catch (Exception e) {
                e.printStackTrace();
            }
            return null;
        }

        @Override
        public DataFrame sum() throws CustomException {
            try {
                message("sum");
            } catch (Exception e) {
                e.printStackTrace();
            }
            return null;
        }

        @Override
        public DataFrame std() throws CustomException {
            try {
                message("std");
            } catch (Exception e) {
                e.printStackTrace();
            }
            return null;
        }

        @Override
        public DataFrame var() throws CustomException {
            try {
                message("var");
            } catch (Exception e) {
                e.printStackTrace();
            }
            return null;
        }

        @Override
        public DataFrame apply(Applyable a) throws CustomException {
            return null;
        }

        public DataFrame message(String type_) throws Exception {
            System.out.println("MESSAGE");
            String type = type_;
            Integer port;
            switch (type) {
                case "max":
                    port = 9001;
                    break;
                case "min":
                    port = 9002;
                    break;
                case "sum":
                    port = 9003;
                    break;
                case "mean":
                    port = 9004;
                    break;
                case "std":
                    port = 9005;
                    break;
                case "var":
                    port = 9006;
                    break;
                default:
                    port = 0;
            }
        /*switch (type){
            case "max":
                int port = 9001;
                List<Value[] >list = new ArrayList<>();
                for (int i=0; i<grouped.getGroupDataFrameList().size(); i++){
                    threads.execute(new MaxServer1(grouped.groupDataFrameList.get(i),port,"max"));
                    port++;
                    if (port>9010)
                }


        }*/
//        LinkedList<Object> message = new LinkedList<>();
//        message.add(grouped);
//        message.add(port);
            Pocket p = new Pocket(grouped, type_);
            System.out.println("Type a message: " + port);
            outToServer.writeObject(p);
            DataFrame output = (DataFrame) inFromServer.readObject();
            System.out.println("Server response :");
            output.print();
            close();
            return output;


        }
    }
    private void close() throws IOException{
        outToServer.close();
        inFromServer.close();
//        stdIn.close();
        echoSocket.close();
    }

    public class GroupByDataFrame implements GroupBy {
        LinkedList<DataFrame> groupDataFrameList;
        String[] columns;
        Class<? extends Value>[] types;
        ArrayList<Integer> groupedCols;
        ArrayList<String> groupedColsNames = new ArrayList<>();

        public LinkedList<DataFrame> getGroupDataFrameList() {
            return groupDataFrameList;
        }

        public String[] getColumns() {
            return columns;
        }

        public Class<? extends Value>[] getTypes() {
            return types;
        }

        public ArrayList<Integer> getGroupedCols() {
            return groupedCols;
        }

        public ArrayList<String> getGroupedColsNames() {
            return groupedColsNames;
        }

        public GroupByDataFrame(LinkedList<DataFrame> linkedList, String[] colnames, Class<? extends Value>[] coltypes, ArrayList<Integer> groupedCols) {
            this.groupDataFrameList = linkedList;
            this.columns = colnames;
            this.types = coltypes;
            this.groupedCols = groupedCols;
            for (int i : groupedCols) groupedColsNames.add(columns[i]);
        }

        public GroupByDataFrame(String[] cols, Class[] types, ArrayList<Integer> groupedCols) {
            groupDataFrameList = new LinkedList<>();
            columns = cols;
            this.types = types;
            this.groupedCols = groupedCols;

        }

        public void addDF(DataFrame df) {
            groupDataFrameList.add(df);
        }

        public void connect(){

        }

        /* no threads */
        @Override
        public DataFrame max() {
            DataFrame output = new DataFrame(columns, types);

            for (DataFrame dataFrame : groupDataFrameList) {
                int index = 0;
                int currentCol = 0;

                Value[] currentRow = new Value[columns.length];
                for (Column column : dataFrame.dataframe) {
                    if (groupedCols.contains(currentCol++)) {
                        currentRow[index++] = column.getArrayList().get(0);
                    } else {
                        currentRow[index++] = Collections.max(column.getArrayList());
                    }
                }
                output.add(currentRow.clone());

            }
            return output;
        }

        @Override
        public DataFrame min() {
            DataFrame output = new DataFrame(columns, types);
            for (DataFrame dataFrame : groupDataFrameList) {
                int currentCol = 0;
                int index = 0;
                Value[] currentRow = new Value[columns.length];
                for (Column column : dataFrame.dataframe) {
                    if (groupedCols.contains(currentCol++)) {
                        currentRow[index++] = column.getArrayList().get(0);
                    } else {
                        currentRow[index++] = Collections.min(column.getArrayList());
                    }
                }
                output.add(currentRow.clone());
            }
            return output;
        }

        @Override
        public DataFrame mean() {
            DataFrame output = CreateDataFrameOfSpecifiedIndexes();

            //Value.ValueBuilder[] builders = new Value.ValueBuilder[output.dataframe.size()];
            //for (int i = 0; i < builders.length; i++) builders[i] = Value.builder(output.types[i]);
            for (DataFrame dataFrame : groupDataFrameList) {
                try {

                    int index = 0;
                    int currentCol = 0;

                    Value[] currentRow = new Value[output.dataframe.size()];

                    for (Column column : dataFrame.dataframe) {
                        if (groupedCols.contains(currentCol++)) currentRow[index++] = column.getArrayList().get(0);
                        else if (column.getType() == StringHolder.class || column.getType() == DateTimeHolder.class)
                            continue;
                        else {
                            Value sum = column.getArrayList().get(0);
                            for (int i = 1; i < column.size(); ++i) {
                                sum = sum.add(column.getArrayList().get(i));
                            }
                            Value mean = sum.div(new IntHolder(column.size()));
//                        System.out.println(mean.getClass());
                            currentRow[index++] = mean;
                        }
                    }
                    output.add(currentRow.clone());
                }catch (CustomException e){e.printStackTrace();}
            }

            return output;
        }

        @Override
        public DataFrame sum() throws CustomException {
            DataFrame output = CreateDataFrameOfSpecifiedIndexes();

            /// Value.ValueBuilder[] builders = new Value.ValueBuilder[output.dataframe.size()];
            // for (int i = 0; i < builders.length; i++) builders[i] = Value.builder(output.types[i]);

            for (DataFrame dataFrame : groupDataFrameList) {
                int index = 0;
                int currentCol = 0;

                Value[] currentRow = new Value[output.dataframe.size()];
                for (Column column : dataFrame.dataframe) {
                    if (groupedCols.contains(currentCol++)) currentRow[index++] = column.getArrayList().get(0);
                    else if (column.getType() == StringHolder.class || column.getType() == DateTimeHolder.class) continue;
                    else {
                        Value sum = column.getArrayList().get(0);
                        for (int i = 1; i < column.size(); ++i) {
                            sum = sum.add(column.getArrayList().get(i));
                        }
                        currentRow[index++] = sum;
                    }
                }
                output.add(currentRow.clone());
            }
            return output;
        }

        @Override
        public DataFrame std() throws CustomException {
            DataFrame output = CreateDataFrameOfSpecifiedIndexes();
            DataFrame mean = mean();
            Value.ValueBuilder[] builders = new Value.ValueBuilder[output.dataframe.size()];
            for (int i = 0; i < builders.length; i++) builders[i] = Value.builder(output.types[i]);

            int currentDf = 0;
            IntHolder exp = new IntHolder(2);
            for (DataFrame dataFrame : groupDataFrameList) {
//                System.out.println("wejscie do wątku "+dataFrame.getDataframe().get(1).getArrayList().get(0));

                int index = 0;
                int currentCol = 0;
                Value[] currentRow = new Value[output.dataframe.size()];
                for (Column column : dataFrame.dataframe) {
                    if (groupedCols.contains(currentCol)) {
                        currentRow[index++] = column.getArrayList().get(0);
                    } else if (column.getType() == StringHolder.class || column.getType() == DateTimeHolder.class) {
                        currentCol++;
                        continue;
                    } else {

                        Value currentmean = mean.getRecord(currentDf)[index];
                        Value sum = builders[index].build((column.getArrayList().get(0).sub(currentmean)).pow(exp).toString());
                        for (int i = 1; i < column.size(); ++i) {
//                            System.out.println(dataFrame.get("id").getArrayList().get(i)+ " " + sum +" "+(column.getArrayList().get(i).sub(currentmean)).pow(exp));
                            sum = sum.add((column.getArrayList().get(i).sub(currentmean)).pow(exp));
                        }

                        currentRow[index] = builders[index++].build(sum.div(new IntHolder(column.size())).pow(new DoubleHolder(0.5)).toString());

                    }
                    currentCol++;
                }
                output.add(currentRow.clone());
                currentDf++;
//                System.out.println("wyjscie z wątku "+dataFrame.getDataframe().get(1).getArrayList().get(0));

            }
            return output;
        }


        @Override
        public DataFrame var() throws CustomException {
            DataFrame output = CreateDataFrameOfSpecifiedIndexes();
            DataFrame std = std();
            IntHolder exp = new IntHolder(2);
            int currentDf = 0;
            for (DataFrame dataFrame : groupDataFrameList) {
                int index = 0;
                int currentCol = 0;
                Value[] currentRow = new Value[output.dataframe.size()];
                for (Column column : dataFrame.dataframe) {
                    if (groupedCols.contains(currentCol)) currentRow[index++] = column.getArrayList().get(0);
                    else if (column.getType() == StringHolder.class || column.getType() == DateTimeHolder.class) {
                        currentCol++;
                        continue;
                    } else {
                        Value currentstd = std.getRecord(currentDf)[index];
                        currentRow[index++] = currentstd.pow(exp);
                    }
                    currentCol++;
                }
                output.add(currentRow.clone());
                currentDf++;
            }
            return output;
        }

        @Override
        public DataFrame apply(Applyable a) throws CustomException {
            DataFrame output = new DataFrame(columns, types);
            for (DataFrame dataFrame : groupDataFrameList) {
                DataFrame current = a.apply(dataFrame);
                for (int i = 0; i < current.size(); ++i)
                    output.add(current.getRecord(i));
            }
            return output;
        }


        /**
         * @return dataframe of columns that were being grouped and columns that are not string and datetime
         */
        private DataFrame CreateDataFrameOfSpecifiedIndexes() {
            ArrayList<Integer> validIndexesOfColumns = new ArrayList<>(groupedCols); //valid columns
            int currentIndexOfCol = 0;
            for (Class<? extends Value> type : types) {
                if (type != StringHolder.class && type != DateTimeHolder.class) validIndexesOfColumns.add(currentIndexOfCol);
                currentIndexOfCol++;
            }
            String[] cols = new String[validIndexesOfColumns.size()];
            Class<? extends Value>[] typs = new Class[validIndexesOfColumns.size()];
            for (int i = 0; i < validIndexesOfColumns.size(); i++) {
                cols[i] = columns[validIndexesOfColumns.get(i)];
                typs[i] = types[validIndexesOfColumns.get(i)];
            }
            return new DataFrame(cols, typs);

        }
    }


}

class Pocket implements Serializable{
    DataFrame.GroupByDataFrame dataFrame;
    int port;
    String s;
    public Pocket(DataFrame.GroupByDataFrame df, int p){dataFrame = df; port =p;};
    public Pocket(DataFrame.GroupByDataFrame df, String s){dataFrame = df; this.s = s;};
}
