package dataframe;

import dataframe.exceptions.CustomException;
import dataframe.groupby.Applyable;
import dataframe.groupby.GroupBy;
import dataframe.value.*;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class DataFrameThreads extends DataFrame {
    public DataFrameThreads() {
        super();
    }

    public DataFrameThreads(String[] namesOfColumns, Class<? extends Value>[] typesOfColumns) {
        super(namesOfColumns, typesOfColumns);
    }

    public DataFrameThreads(Column[] kolumny) {
        super(kolumny);
    }

    public DataFrameThreads(String filename, Class<? extends Value>[] typesOfColumns, String[] colnames) {
        super(filename, typesOfColumns, colnames);
    }

    public DataFrameThreads(String filename, Class<? extends Value>[] typesOfColumns) {
        super(filename, typesOfColumns);
    }

    public DataFrameThreads(String filename, Class<? extends Value>[] typesOfColumns, boolean header) {
        super(filename, typesOfColumns, header);
    }

    public void awaitTerminationAfterShutdown(ExecutorService threadPool) {
        threadPool.shutdown();
        try {
            if (!threadPool.awaitTermination(60, TimeUnit.SECONDS)) {
                threadPool.shutdownNow();
            }
        } catch (InterruptedException ex) {
            threadPool.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
    private class GroupByThread implements Runnable{
        List<Column> columns;
        int from,to;
        Map<List<Value>, DataFrame> map;
        GroupByThread(List<Column> columns_, int from_, int to_, Map<List<Value>, DataFrame> map_){
            columns = columns_;
            from = from_;
            to = to_;
            map = map_;
        }
        @Override
        public void run() {
            for (int i=from; i<=to; i++){

                List<Value> values = new ArrayList<>(columns.size());

                for (Column column : columns) {
                    values.add(column.getArrayList().get(i));
                }

                if (!map.containsKey(values)) {
                    try{
                        synchronized (map){
                            map.put(values, iloc(i));
                        }
                    } catch (Exception e ) {
                        e.printStackTrace();
                    }

                } else {
                    synchronized (map){
                        map.get(values).add(getRecord(i));}
                }
            }
        }
    }


    @Override
    public GroupByDataFrame groupby(String... colnames) throws Exception {
        //        HashMap<List<Value>, DataFrame> map = new HashMap<>(colnames.length);
        Map<List<Value>, DataFrame> map = new ConcurrentHashMap<>(colnames.length);

        List<Column> columns1 = Arrays.stream(colnames).map(this::get).collect(Collectors.toList());
        ArrayList<Integer> indexes = GetIndexesOfColumns(colnames);
//        Thread[] arrayOfThreads = new Thread[n];
//        int n = this.n*2;
        ExecutorService executorService = Executors.newFixedThreadPool(n);
        int counter = 0;

        for (int i=0; i<size(); i=i+size()/n+1){
//            System.out.println(i+size()/10);
//            arrayOfThreads[counter] = new Thread(new GroupByThread(columns1,i,(i+size()/10>size()) ? size()-1 : i+size()/10,map));
//            arrayOfThreads[counter++].start();
            executorService.execute(new Thread(new GroupByThread(columns1,i,(i+size()/n>size()) ? size()-1 : i+size()/n,map)));
        }
        /*for (Thread t:arrayOfThreads){
            t.join();
        }*/
        awaitTerminationAfterShutdown(executorService);
//        executorService.shutdownNow();
        return new GroupByDataFrame(new LinkedList<>(map.values()), this.columns, types, indexes);
    }

    public class GroupByDataFrame extends DataFrame.GroupByDataFrame implements GroupBy, Serializable {
        LinkedList<DataFrame> groupDataFrameList;
        String[] columns;
        Class<? extends Value>[] types;
        ArrayList<Integer> groupedCols;
        ArrayList<String> groupedColsNames = new ArrayList<>();
        ExecutorService threads = Executors.newFixedThreadPool(n);
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

        public GroupByDataFrame(){}
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

        class ThreadDF{
            DataFrame workable, out;

            ThreadDF(DataFrame current, DataFrame output) {
                workable = current;
                out = output;
            }
        }
        class MaxThread extends ThreadDF implements Runnable {
            MaxThread(DataFrame current, DataFrame out){super(current,out);};
            @Override
            public void run() {
                int index = 0;
                int currentCol = 0;

                Value[] currentRow = new Value[columns.length];
                for (Column column : workable.dataframe) {
                    if (groupedCols.contains(currentCol++)) {
                        currentRow[index++] = column.getArrayList().get(0);
                    } else {
                        currentRow[index++] = Collections.max(column.getArrayList());
                    }
                }
                synchronized (out){
                    out.add(currentRow.clone());
                }
            }
        }

        class MinThread extends ThreadDF implements Runnable {
            MinThread(DataFrame current, DataFrame out){super(current,out);};
            @Override
            public void run() {
//                System.out.println("wejscie do wątku "+workable.getDataframe().get(1).getArrayList().get(0));
                int index = 0;
                int currentCol = 0;

                Value[] currentRow = new Value[columns.length];
                for (Column column : workable.dataframe) {
                    if (groupedCols.contains(currentCol++)) {
                        currentRow[index++] = column.getArrayList().get(0);
                    } else {
                        currentRow[index++] = Collections.min(column.getArrayList());
                    }
                }
                synchronized (out){
                    out.add(currentRow.clone());
                }
//                System.out.println("wyjscie do wątku "+workable.getDataframe().get(1).getArrayList().get(0));
            }
        }

        class MeanThread extends ThreadDF implements Runnable {
            MeanThread(DataFrame current, DataFrame out){super(current,out);};
            MeanThread(DataFrame current, DataFrame out, Value[][] a,int i){super(current,out); currentDf=i;}
            int currentDf;
            @Override
            public void run() {
                try{
//                    System.out.println("mean1");
                    int index = 0;
                    int currentCol = 0;

                    Value[] currentRow = new Value[out.dataframe.size()];

                    for (Column column : workable.dataframe) {
                        if (groupedCols.contains(currentCol++)) currentRow[index++] = column.getArrayList().get(0);
                        else if (column.getType() == StringHolder.class || column.getType() == DateTimeHolder.class)
                            continue;
                        else {
                            Value sum = column.getArrayList().get(0);
                            for (int i = 1; i < column.size(); ++i) {
                                sum = sum.add(column.getArrayList().get(i));
                            }
                            Value mean = sum.div(new IntHolder(column.size()));
                            currentRow[index++] = mean;
                        }
                    }

                    synchronized (out){
                        out.add(currentRow.clone());
                    }

                }
                catch (CustomException e){e.printStackTrace();}
//                System.out.println("mean2");
            }
        }

        class SumThread extends ThreadDF implements Runnable {
            SumThread(DataFrame current, DataFrame out,Value.ValueBuilder[] valueBuilders){super(current,out);builders = valueBuilders.clone();};
            Value.ValueBuilder[] builders;
            @Override
            public void run() {
                try{
                    int index = 0;
                    int currentCol = 0;

                    Value[] currentRow = new Value[out.dataframe.size()];
                    for (Column column : workable.dataframe) {
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
                    synchronized (out){
                        out.add(currentRow.clone());}
                }
                catch (CustomException e){e.printStackTrace();}
            }
        }

        class StdThread extends ThreadDF implements Runnable {
            int currentDf ;
            Value.ValueBuilder[] builders;
            IntHolder exp;
            DataFrame mean;
            StdThread(DataFrame current, DataFrame out, int currentDf, Value.ValueBuilder[] builders, IntHolder exp, DataFrame mean){super(current,out); this.currentDf = currentDf; this.builders = builders.clone(); this.exp = exp; this.mean = mean;};
            @Override
            public void run() {
                try{
                    int index = 0;
                    int currentCol = 0;
                    Value[] currentRow = new Value[out.dataframe.size()];
                    for (Column column : workable.dataframe) {
                        if (groupedCols.contains(currentCol)) {
                            currentRow[index++] = column.getArrayList().get(0);
                        } else if (column.getType() == StringHolder.class || column.getType() == DateTimeHolder.class) {
                            currentCol++;
                            continue;
                        } else {
                            Value currentmean = mean.getRecord(currentDf)[index];

                            Value sum = builders[index].build((column.getArrayList().get(0).sub(currentmean)).pow(exp).toString());
                            for (int i = 1; i < column.size(); ++i) {
                                sum = sum.add((column.getArrayList().get(i).sub(currentmean)).pow(exp));
                            }
                            currentRow[index] = builders[index++].build(sum.div(new IntHolder(column.size())).pow(new DoubleHolder(0.5)).toString());
                        }
                        currentCol++;
                    }
                    synchronized (out){
                        out.add(currentRow.clone());
                    }}
                catch (CustomException e) {e.printStackTrace();}

            }
        }

        class VarThread extends ThreadDF implements Runnable {
            int currentDf ;
            Value.ValueBuilder[] builders;
            IntHolder exp;
            DataFrame mean;
            VarThread(DataFrame current, DataFrame out, int currentDf, Value.ValueBuilder[] builders, IntHolder exp, DataFrame mean){super(current,out); this.currentDf = currentDf; this.builders = builders.clone(); this.exp = exp; this.mean = mean;};
            @Override
            public void run() {
                try{
                    int index = 0;
                    int currentCol = 0;
                    Value[] currentRow = new Value[out.dataframe.size()];
                    for (Column column : workable.dataframe) {
                        if (groupedCols.contains(currentCol)) {
                            currentRow[index++] = column.getArrayList().get(0);
                        } else if (column.getType() == StringHolder.class || column.getType() == DateTimeHolder.class) {
                            currentCol++;
                            continue;
                        } else {
                            Value currentmean = mean.getRecord(currentDf)[index];

                            Value sum = builders[index].build((column.getArrayList().get(0).sub(currentmean)).pow(exp).toString());
                            for (int i = 1; i < column.size(); ++i) {
                                sum = sum.add((column.getArrayList().get(i).sub(currentmean)).pow(exp));
                            }
                            currentRow[index] = builders[index++].build(sum.div(new IntHolder(column.size())).pow(new DoubleHolder(1)).toString());
                        }
                        currentCol++;
                    }
                    synchronized (out){
                        out.add(currentRow.clone());
                    }}
                catch (CustomException e) {e.printStackTrace();}

            }
        }


        public DataFrame max2() {
            DataFrame output = new DataFrame(columns, types);;
            List<Callable<List<Value>>> callables = new ArrayList<>();
            for (DataFrame df: groupDataFrameList){
                callables.add(()->{
                    List<Value> toAdd = new ArrayList<>();
                    int currentCol = 0;

                    for (Column column : df.dataframe) {
                        if (groupedCols.contains(currentCol++)) {
                            toAdd.add(column.getArrayList().get(0));
                        } else {
                            Value max = column.getArrayList().get(0);
                            for (Iterator<Value> iterator =  column.getArrayList().iterator(); iterator.hasNext();){
                                Value current = iterator.next();
                                if (current.gte(max))
                                    max = current;
                            }
                            toAdd.add(max);
                        }

                    }
                    return toAdd;
                });
            }
            List<List<Value>> aggregateDataFrameValues = new ArrayList<>();
            List<Future<List<Value>>> futureValues;
            try {
                futureValues = threads.invokeAll(callables);
                for (Future<List<Value>> value : futureValues) {
                    aggregateDataFrameValues.add(value.get());
                }
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
            threads.shutdown();
            for (List<Value> result : aggregateDataFrameValues) {
                output.add(result);
            }
            return output;
        }
        public DataFrame max() {
            DataFrame output = new DataFrame(columns, types);;
            /*for (DataFrame df : groupDataFrameList) {
                new Thread(new MaxThread(df,output)).start();
            }*/
            Thread[] array = new Thread[groupDataFrameList.size()];
            int currentDf = 0;
           /* for (DataFrame df : groupDataFrameList) {
                // f(df,output,currentDf,builders,exp,mean);
                array[currentDf++] = new Thread(new MaxThread(df,output));
                array[currentDf-1].start();
            }
            for(Thread t:array){
                try {
                    t.join();
                }
                catch (InterruptedException e ){e.printStackTrace();}
            }
            /*for (DataFrame df : groupDataFrameList) {
                new Thread(new MeanThread(df,output)).start();}*/
            for (DataFrame df: groupDataFrameList){
                threads.execute(new Thread(new MaxThread(df,output)));
            }
            awaitTerminationAfterShutdown(threads);
//           threads.shutdownNow();
            return output;
            //return output;
        }
        public DataFrame max1() {
            DataFrame output = new DataFrame(columns, types);;
            Thread[] array = new Thread[groupDataFrameList.size()];
            int currentDf = 0;
            for (DataFrame df : groupDataFrameList) {
                array[currentDf++] = new Thread(new MaxThread(df,output));
                array[currentDf-1].start();
            }
            for(Thread t:array){
                try {
                    t.join();
                }
                catch (InterruptedException e ){e.printStackTrace();}
            }
            return output;
            //return output;
        }

        public DataFrame min() {
            DataFrame output = new DataFrame(columns, types);;
            /*for (DataFrame df : groupDataFrameList) {
                new Thread(new MinThread(df,output)).start();
            }*/
            Thread[] array = new Thread[groupDataFrameList.size()];
            int currentDf = 0;
            for (DataFrame df : groupDataFrameList) {
                // f(df,output,currentDf,builders,exp,mean);
                threads.execute(new Thread(new MinThread(df, output)));
            }
//            threads.shutdownNow();
            awaitTerminationAfterShutdown(threads);
            return output;
            //return output;
        }
        public DataFrame min1() {
            DataFrame output = new DataFrame(columns, types);;
            /*for (DataFrame df : groupDataFrameList) {
                new Thread(new MinThread(df,output)).start();
            }*/
            Thread[] array = new Thread[groupDataFrameList.size()];
            int currentDf = 0;
            for (DataFrame df : groupDataFrameList) {
                // f(df,output,currentDf,builders,exp,mean);
                array[currentDf++] = new Thread(new MinThread(df,output));
                array[currentDf-1].start();
            }
            for(Thread t:array){
                try {
                    t.join();
                }
                catch (InterruptedException e ){e.printStackTrace();}
            }
            /*for (DataFrame df : groupDataFrameList) {
                new Thread(new MeanThread(df,output)).start();}*/
            return output;
            //return output;
        }
        public DataFrame mean1() {
            DataFrame output = CreateDataFrameOfSpecifiedIndexes();
            Thread[] array = new Thread[groupDataFrameList.size()];
            int currentDf = 0;
            Value[][] array1 = new Value[groupDataFrameList.size()][];
            for (DataFrame df : groupDataFrameList) {
                // f(df,output,currentDf,builders,exp,mean);
                array[currentDf] = new Thread(new MeanThread(df,output,array1,currentDf));
                array[currentDf++].start();

            }

            for (int i=0; i<array.length; i++){
                try {
                    array[i].join();

                } catch (Exception e){}
//                output.add(array1[i]);
            }
            return output;
        }
        public DataFrame mean() {
            DataFrame output = CreateDataFrameOfSpecifiedIndexes();
            Thread[] array = new Thread[groupDataFrameList.size()];
            int currentDf = 0;
            Value[][] array1 = new Value[groupDataFrameList.size()][];
//            List<Callable<>> taskList = new ArrayList<>();
            for (DataFrame df : groupDataFrameList) {
                threads.execute(new Thread(new MeanThread(df, output, array1, currentDf)));
            }
//            threads.shutdownNow();
            awaitTerminationAfterShutdown(threads);
           /* try {

                while (!threads.awaitTermination(24L, TimeUnit.HOURS)) {
                    System.out.println("Not yet. Still waiting for termination");
                }
            } catch (InterruptedException e){e.printStackTrace();}*/

            return output;
        }

        public DataFrame sum() {
            DataFrame output = CreateDataFrameOfSpecifiedIndexes();
            threads = Executors.newCachedThreadPool();
            Value.ValueBuilder[] builders = new Value.ValueBuilder[output.dataframe.size()];
            for (int i = 0; i < builders.length; i++) builders[i] = Value.builder(output.types[i]);
            for (DataFrame df : groupDataFrameList) {
                threads.execute(new Thread(new SumThread(df,output,builders)));
            }
//            threads.shutdownNow();
            awaitTerminationAfterShutdown(threads);
            return output;
            //return output;
        }
        public  DataFrame sum1() {
            DataFrame output = CreateDataFrameOfSpecifiedIndexes();
            Value.ValueBuilder[] builders = new Value.ValueBuilder[output.dataframe.size()];
            for (int i = 0; i < builders.length; i++) builders[i] = Value.builder(output.types[i]);
            Thread[] array = new Thread[groupDataFrameList.size()];
            int currentDf = 0;
            for (DataFrame df : groupDataFrameList) {
                // f(df,output,currentDf,builders,exp,mean);
                array[currentDf++] = new Thread(new SumThread(df,output,builders));
                array[currentDf-1].start();
            }
            for(Thread t:array){
                try {
                    t.join();
                }
                catch (InterruptedException e ){e.printStackTrace();}
            }
            return output;

        }

        public  DataFrame std() {
            DataFrame output = CreateDataFrameOfSpecifiedIndexes();
            DataFrame mean = mean();
//            System.out.println(mean);
            threads = Executors.newFixedThreadPool(n*2);
            Value.ValueBuilder[] builders = new Value.ValueBuilder[output.dataframe.size()];
            for (int i = 0; i < builders.length; i++) builders[i] = Value.builder(output.types[i]);
            int currentDf = 0;
            IntHolder exp = new IntHolder(2);
            for (DataFrame df : groupDataFrameList) {
                threads.execute(new Thread(new StdThread(df,output,currentDf++,builders,exp,mean)) );
            }
            awaitTerminationAfterShutdown(threads);
            return output;
        }

        public  DataFrame std1() {
            DataFrame output = CreateDataFrameOfSpecifiedIndexes();

            DataFrame mean = mean1();

            Value.ValueBuilder[] builders = new Value.ValueBuilder[output.dataframe.size()];
            for (int i = 0; i < builders.length; i++) builders[i] = Value.builder(output.types[i]);
            Thread[] array = new Thread[groupDataFrameList.size()];
            int currentDf = 0;
            IntHolder exp = new IntHolder(2);
            for (DataFrame df : groupDataFrameList) {
                // f(df,output,currentDf,builders,exp,mean);
                array[currentDf] = new Thread(new StdThread(df,output,currentDf++,builders,exp,mean));
                array[currentDf-1].start();
            }
            for(Thread t:array){
                try {
                    t.join();
                }
                catch (InterruptedException e ){e.printStackTrace();}
            }
            return output;
        }
        public DataFrame var(){
            DataFrame output = CreateDataFrameOfSpecifiedIndexes();
            DataFrame std = mean();
            Value.ValueBuilder[] builders = new Value.ValueBuilder[output.dataframe.size()];
            for (int i = 0; i < builders.length; i++) builders[i] = Value.builder(output.types[i]);
            threads = Executors.newFixedThreadPool(n);
            IntHolder exp = new IntHolder(2);
            int currentDf = 0;
            for (DataFrame df : groupDataFrameList) {
                threads.execute(new Thread(new VarThread(df,output,currentDf++,builders,exp,std)));
            }
            awaitTerminationAfterShutdown(threads);
            return output;
        }

        public DataFrame var1(){
            DataFrame output = CreateDataFrameOfSpecifiedIndexes();
            DataFrame std = std1();
            IntHolder exp = new IntHolder(2);
            int currentDf = 0;
            /*for (DataFrame df : groupDataFrameList) {
                new Thread(new VarThread(df,output,currentDf++,exp,std)).start();
            }*/
            Thread[] array = new Thread[groupDataFrameList.size()];
            Value.ValueBuilder[] builders = new Value.ValueBuilder[output.dataframe.size()];
            for (int i = 0; i < builders.length; i++) builders[i] = Value.builder(output.types[i]);
            for (DataFrame df : groupDataFrameList) {
                // f(df,output,currentDf,builders,exp,mean);
                array[currentDf] = new Thread(new VarThread(df,output,currentDf++,builders,exp,std));
                array[currentDf-1].start();
            }
            for(Thread t:array){
                try {
                    t.join();
                }
                catch (InterruptedException e ){e.printStackTrace();}
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
        public DataFrame CreateDataFrameOfSpecifiedIndexes() {
            ArrayList<Integer> validIndexesOfColumns = new ArrayList<>(groupedCols); //valid columns
            int currentIndexOfCol = 0;
            for (Class<? extends Value> type : types) {
                if (type != StringHolder.class && type != DateTimeHolder.class) validIndexesOfColumns.add(currentIndexOfCol);
                currentIndexOfCol++;
            }
            Collections.sort(validIndexesOfColumns);
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
