package dataframe;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.util.*;

public class DataFrame {

    ArrayList<Column> dataframe;
    String[] columns;
    Class<? extends Value>[] types;

    public DataFrame(String[] namesOfColumns, Class<? extends Value>[] typesOfColumns){
        if (namesOfColumns.length!=typesOfColumns.length){
            System.exit(1);
        }
        columns = namesOfColumns;
        types = typesOfColumns;
        dataframe = new ArrayList<>();
        for (int i=0; i<namesOfColumns.length; ++i){
            if (Value.class.isAssignableFrom(types[i])) {
                dataframe.add(new Column(namesOfColumns[i], typesOfColumns[i]));
            }
            else System.exit(2);
        }
    }

    public DataFrame(Column[] kolumny){
        dataframe = new ArrayList<>();
        columns=new String[kolumny.length];
        types = new Class[kolumny.length];
        for (int i=0; i<kolumny.length; ++i) {
            columns[i]=kolumny[i].getName();
            types[i]=kolumny[i].getType();
            dataframe.add(kolumny[i]);
            if (dataframe.get(i).getArrayList().size()!=dataframe.get(0).getArrayList().size())
                throw new RuntimeException("Invalid length of column");
        }
    }

    public DataFrame(String filename, Class<? extends Value>[] typesOfColumns){
        this(filename,typesOfColumns,true);
    }

    public DataFrame(String filename, Class<? extends Value>[] typesOfColumns, boolean header) {
        try{
            FileInputStream fstream = new FileInputStream(filename);
            BufferedReader br = new BufferedReader(new InputStreamReader(fstream));

            dataframe = new ArrayList<>();
            columns = new String[typesOfColumns.length];
            types = typesOfColumns;
            if (header) {
                columns = br.readLine().split(",");
            } else {
                System.out.println("Enter names of columns");
                Scanner scanner = new Scanner(System.in);
                for (int i = 0; i < columns.length; ++i) {
                    columns[i] = scanner.next();
                }
            }

            for (int i = 0; i < typesOfColumns.length; ++i) {
                dataframe.add(new Column(columns[i], types[i]));
            }

            String strLine;
            Value[] values = new Value[dataframe.size()];
            Value.ValueBuilder[] builders = new Value.ValueBuilder[dataframe.size()];
            for (int i = 0; i < builders.length; i++) {
                builders[i] = Value.builder(types[i]);
            }


            while ((strLine = br.readLine()) != null) {
                String[] str = strLine.split(",");
                for (int i = 0; i < str.length; i++) {
                    values[i] = builders[i].build(str[i]);
                }
                add(values.clone());
            }

            br.close();
        } catch (IOException e){e.printStackTrace();}


    }

    public void add(Value[] values){
        if (values.length!=dataframe.size()){
            throw new RuntimeException("Invalid length of input!!!");
        }
        int iterator = 0;
        for (Column col:dataframe){
            col.addElement(values[iterator++]);
        }
    }

    public void print(){
        for (String string:columns){
            System.out.print(string+" ");
        }
        System.out.println();
        for (int i=0; i<size(); i++){
            for (Column col:dataframe){
                System.out.print(col.getArrayList().get(i)+" ");
            }
            System.out.println();
        }
    }

    public int size(){
        return this.dataframe.get(0).size();
    }

    public Column get(String colname){
        for (Column col:dataframe){
            if (Objects.equals(col.getName(),colname)){
                return col;
            }
        }
        throw new RuntimeException("Column not found!");
    }

    public Value[] getRecord(int index){
        Value[] tab = new Value[this.dataframe.size()];
        int it=0;
        for (Column column:dataframe){
            tab[it++]=column.getArrayList().get(index);
        }
        return tab;
    }

    public DataFrame get(String [] cols, boolean copy){
        Column[] tab = new Column[cols.length];
        for (int i=0; i<tab.length; ++i){
            if (!copy){ //shallow copy
                tab[i] = this.get(cols[i]);
            }
            else{ //deep copy
                tab[i] = new Column(get(cols[i]));
            }
        }
        //for (Column column:tab) System.out.println(column.getName()+column.getType());
        DataFrame dataFrame = new DataFrame(tab);
        return dataFrame;

    }

    public DataFrame iloc(int i) throws Exception{
        return iloc(i,i);

    }

    public DataFrame iloc(int from, int to) throws Exception{
        if(from<0 || from>=size())
            throw new IndexOutOfBoundsException("No such index: "+from);

        else if(to<0 || to>=size())
            throw new IndexOutOfBoundsException("No such index: "+to);

        else if(to<from)
            throw new IndexOutOfBoundsException("unable to create range from "+from+" to "+to);

        DataFrame nowydf = new DataFrame(columns,types);
        Value[] tab = new Value[dataframe.size()];
        for (int i=from; i<=to; ++i){
            for (int j=0; j<tab.length; j++){
                tab[j] = dataframe.get(j).getArrayList().get(i);
            }
            nowydf.add(tab);

        }
        return nowydf;
    }

    /**
     *
     * @param colnames - cilumns that we group by
     * @return GroupbyDataFrame linekd list of grouped dataframes
     * @throws Exception //TODO: rethink exceptions
     */
    public GroupByDataFrame groupby(String[] colnames) throws Exception{
        if (colnames.length>columns.length) throw new Exception();
        LinkedList<DataFrame> dataFrameLinkedList = new LinkedList<>();
        dataFrameLinkedList.add(this);
        //int[] indexes = new int[colnames.length];
        ArrayList<Integer> indexes = new ArrayList<>();
        int index =0;
        for (String str: colnames){
            boolean found = false;
            for (int i=0; i<columns.length; ++i){
                if (Objects.equals(str,columns[i])) {indexes.add(index++,i); found = true;}
            }
            if (!found) throw new Exception("Invalid column name");
        }
        for (int j:indexes){
            LinkedList<DataFrame>result = new LinkedList<>();
            for (DataFrame dataFrame: dataFrameLinkedList){
                Set<Value> UniqueValues = new HashSet<>(dataFrame.dataframe.get(j).getArrayList());
                List<Value> values = new ArrayList<>(UniqueValues);
                Collections.sort(values);
                //for (Value value:listOfUniqueValues) System.out.println(value);
                for (Value value:values){
                    DataFrame current = getDataFrameOfCertainValue(dataFrame,value,j);
                    if (current.size()!=0) result.add(current);
                }

                dataFrameLinkedList = new LinkedList<>(result);
            }
        }
        //for (int i:indexes) System.out.println(i);
        return new GroupByDataFrame(dataFrameLinkedList,columns,types,indexes);

    }


    private DataFrame getDataFrameOfCertainValue(DataFrame df, Value v, int indexOfColumn) throws Exception{
        DataFrame dataFrame= new DataFrame(columns,types);
        int indexOfRow=0;
        for (Value value:df.dataframe.get(indexOfColumn).getArrayList()){
            if (value.equals(v)){
                dataFrame.add(df.getRecord(indexOfRow));
            }
            indexOfRow++;
        }
        return dataFrame;
    }

    public class GroupByDataFrame implements GroupBy{
        LinkedList<DataFrame> groupDataFrameList;
        String[] columns;
        Class<? extends Value>[] types;
        ArrayList<Integer> groupedCols;
        ArrayList<String> groupedColsNames = new ArrayList<>();


        GroupByDataFrame(LinkedList<DataFrame> linkedList, String[] colnames, Class<? extends Value>[] coltypes, ArrayList<Integer> groupedCols){
            this.groupDataFrameList=linkedList;
            this.columns = colnames;
            this.types = coltypes;
            this.groupedCols = groupedCols;
            for (int i:groupedCols) groupedColsNames.add(columns[i]);
        }

        @Override
        public DataFrame max(){
            DataFrame output = new DataFrame(columns,types);

            for (DataFrame dataFrame: groupDataFrameList){
                int index=0;
                int currentCol =0;

                Value[] currentRow = new Value[columns.length];
                for (Column column:dataFrame.dataframe){
                    if (groupedCols.contains(currentCol++)){
                        currentRow[index++] = column.getArrayList().get(0);
                    }
                    else{
                        currentRow[index++] = Collections.max(column.getArrayList());
                    }
                }
                output.add(currentRow.clone());

            }
            return output;
        }

        @Override
        public DataFrame min(){
            DataFrame output = new DataFrame(columns,types);
            for (DataFrame dataFrame: groupDataFrameList){
                int currentCol =0;
                int index=0;
                Value[] currentRow = new Value[columns.length];
                for (Column column:dataFrame.dataframe){
                    if (groupedCols.contains(currentCol++)){
                        currentRow[index++] = column.getArrayList().get(0);
                    }
                    else{
                        currentRow[index++] = Collections.min(column.getArrayList());
                    }
                }
                output.add(currentRow.clone());
            }
            return output;
        }

        @Override
        public DataFrame mean(){
            DataFrame output = CreateDataFrameOfSpecifiedIndexes();

            Value.ValueBuilder[] builders = new Value.ValueBuilder[output.dataframe.size()];
            for (int i = 0; i < builders.length; i++) builders[i] = Value.builder(output.types[i]);
            for (DataFrame dataFrame: groupDataFrameList){
                int index=0;
                int currentCol =0;

                Value[] currentRow = new Value[output.dataframe.size()];

                for (Column column:dataFrame.dataframe){
                    if (groupedCols.contains(currentCol++)) currentRow[index++] = column.getArrayList().get(0);
                    else if (column.getType()==ValString.class || column.getType() == ValDateTime.class)
                        continue;
                    else {
                        Value sum = column.getArrayList().get(0);
                        for (int i=1 ; i<column.size(); ++i){
                            sum = sum.add(column.getArrayList().get(i));
                        }
                        Value mean = sum.div(new ValInteger(column.size()));
                        currentRow[index++] = mean;
                    }
                }
                output.add(currentRow.clone());
            }

            return output;
        }

        @Override
        public DataFrame sum(){
            DataFrame output = CreateDataFrameOfSpecifiedIndexes();

            Value.ValueBuilder[] builders = new Value.ValueBuilder[output.dataframe.size()];
            for (int i = 0; i < builders.length; i++) builders[i] = Value.builder(output.types[i]);

            for (DataFrame dataFrame: groupDataFrameList){
                int index=0;
                int currentCol =0;

                Value[] currentRow = new Value[output.dataframe.size()];
                for (Column column:dataFrame.dataframe){
                    if (groupedCols.contains(currentCol++)) currentRow[index++] = column.getArrayList().get(0);
                    else if(column.getType()==ValString.class || column.getType() == ValDateTime.class) continue;
                    else {
                        Value sum = column.getArrayList().get(0);
                        for (int i=1 ; i<column.size(); ++i){
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
        public DataFrame std(){
            DataFrame output = CreateDataFrameOfSpecifiedIndexes();
            DataFrame mean = mean();

            Value.ValueBuilder[] builders = new Value.ValueBuilder[output.dataframe.size()];
            for (int i = 0; i < builders.length; i++) builders[i] = Value.builder(output.types[i]);

            int currentDf=0;
            ValInteger exp = new ValInteger(2);
            for (DataFrame dataFrame: groupDataFrameList){
                int index=0;
                int currentCol =0;
                Value[] currentRow = new Value[output.dataframe.size()];
                for (Column column:dataFrame.dataframe){
                    if (groupedCols.contains(currentCol)) currentRow[index++] = column.getArrayList().get(0);
                    else if(column.getType()==ValString.class || column.getType() == ValDateTime.class) continue;
                    else {
                        Value currentmean = mean.getRecord(currentDf)[index];
                        Value sum = (column.getArrayList().get(0).sub(currentmean)).pow(exp);
                        for (int i=1 ; i<column.size(); ++i){
                            sum = sum.add((column.getArrayList().get(i).sub(currentmean)).pow(exp));
                        }

                        currentRow[index] = builders[index++].build(Double.toString(Math.sqrt((double)sum.div(new ValInteger(column.size()-1)).getValue())));
                    }
                    currentCol++;
                }
                output.add(currentRow.clone());
                currentDf++;
            }
            return output;
        }


        @Override
        public DataFrame var(){
            DataFrame output = CreateDataFrameOfSpecifiedIndexes();
            DataFrame std = std();

            Value.ValueBuilder[] builders = new Value.ValueBuilder[output.dataframe.size()];
            for (int i = 0; i < builders.length; i++) builders[i] = Value.builder(output.types[i]);

            ValInteger exp = new ValInteger(2);
            int currentDf=0;
            for (DataFrame dataFrame: groupDataFrameList){
                int index=0;
                int currentCol =0;
                Value[] currentRow = new Value[output.dataframe.size()];
                for (Column column:dataFrame.dataframe){
                    if (groupedCols.contains(currentCol)) currentRow[index++] = column.getArrayList().get(0);
                    else if(column.getType()==ValString.class || column.getType() == ValDateTime.class) continue;
                    else {
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
        public DataFrame apply(Applyable a) {
            DataFrame output = new DataFrame(columns,types);
            for (DataFrame dataFrame: groupDataFrameList){
                DataFrame current = a.apply(dataFrame);
                for (int i=0; i<current.size(); ++i)
                    output.add(current.getRecord(i));
            }
            return output;
        }

        /**
         *
         * @return dataframe of columns that were being grouped and columns that are not string and datetime
         */
        private DataFrame CreateDataFrameOfSpecifiedIndexes(){
            ArrayList<Integer> validIndexesOfColumns = new ArrayList<>(groupedCols); //valid columns
            int currentIndexOfCol = 0;
            for (Class<? extends Value> type:types){
                if (type != ValString.class && type != ValDateTime.class) validIndexesOfColumns.add(currentIndexOfCol);
                currentIndexOfCol++;
            }
            String[] cols = new String[validIndexesOfColumns.size()];
            Class<? extends Value>[] typs = new Class[validIndexesOfColumns.size()];
            for (int i=0; i<validIndexesOfColumns.size(); i++){
                cols[i] = columns[validIndexesOfColumns.get(i)];
                typs[i] = types[validIndexesOfColumns.get(i)];
            }
            return new DataFrame(cols,typs);

        }
    }
}
