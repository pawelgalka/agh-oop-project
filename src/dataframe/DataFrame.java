package dataframe;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Objects;
import java.util.Scanner;

public class DataFrame {

    ArrayList<Column> dataframe;
    String[] columns;
    Class<? extends Value>[] types;


    public DataFrame(String[] namesOfColumns, Class<? extends Value>[] typesOfColumns) throws Exception{
        if (namesOfColumns.length!=typesOfColumns.length){
            throw new IllegalArgumentException();
        }
        columns = namesOfColumns;
        types = typesOfColumns;
        dataframe = new ArrayList<>();
        for (int i=0; i<namesOfColumns.length; ++i){
            if (Value.class.isAssignableFrom(types[i])) {
                dataframe.add(new Column(namesOfColumns[i], typesOfColumns[i]));
            }
            else throw new Exception();
        }
    }

    public DataFrame(Column[] kolumny){
        dataframe = new ArrayList<>();
        columns=new String[kolumny.length];
        for (int i=0; i<kolumny.length; ++i) {
            columns[i]=kolumny[i].getName();
            types[i]=kolumny[i].getType();
            dataframe.add(kolumny[i]);
            if (dataframe.get(i).getArrayList().size()!=dataframe.get(0).getArrayList().size())
                throw new RuntimeException("Invalid length of column");
        }
    }

    public DataFrame(String filename, Class<? extends Value>[] typesOfColumns) throws Exception{
        this(filename,typesOfColumns,true);
    }

    public DataFrame(String filename, Class<? extends Value>[] typesOfColumns, boolean header) throws Exception {
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
        while ((strLine = br.readLine()) != null) {
            String[] str = strLine.split(",");
            for (int i = 0; i < str.length; i++) {
                if (types[i] == ValInteger.class){
                    values[i] = ValInteger.getInstance().create(str[i]);
                }
                if (types[i] == ValDouble.class){
                    values[i] = ValDouble.getInstance().create(str[i]);
                }
                if (types[i] == ValBoolean.class){
                    values[i] = ValBoolean.getInstance().create(str[i]);
                }
                if (types[i] == ValFloat.class){
                    values[i] = ValFloat.getInstance().create(str[i]);
                }
                if (types[i] == ValString.class){
                    values[i] = ValString.getInstance().create(str[i]);
                }
            }
            add(values.clone());
        }

        br.close();
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
            //System.out.println(this.get(cols[i]).getName());
            if (!copy){ //shallow copy
                tab[i] = this.get(cols[i]);
            }
            else{ //deep copy
                tab[i] = new Column(get(cols[i]));
            }
        }
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

}
