package dataframe;

import java.util.ArrayList;

public class Column {
    private String name;
    private Class<? extends Value> type;
    private ArrayList<Value> arrayList;

    public Column(String name, Class<? extends Value> clazz) {
        arrayList = new ArrayList<>();
        this.name = name;
        this.type = clazz;
    }

    public Column(Column kol){
        this.name = kol.getName();
        this.type = kol.getType();
        this.arrayList = kol.getArrayList();
    }

    public void addElement(Value element) {
        if(type.isInstance(element)) {
            arrayList.add(element);
        }
        else {
            try {
                System.out.println(element.toString());
                Thread.sleep(0);
            }
            catch (InterruptedException e){

            }
            throw new IllegalArgumentException();
        }
    }

    Value getCOOElement(int index, Value hide){
        for(int i=0; i<arrayList.size(); ++i){
            COOValue value = (COOValue)arrayList.get(i);
            if (value.getIndex()==index){
                return value.getValue();
            }
        }
        return hide;
    }

    int size(){
        return arrayList.size();
    }

    String getName(){
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    Class<? extends Value> getType(){
        return type;
    }

    public ArrayList<Value> getArrayList() {
        return arrayList;
    }

    String print(){
        return "name: "+name+" type: "+type+" content: "+arrayList;
    }
}
