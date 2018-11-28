package dataframe.groupby;

import dataframe.*;
import dataframe.Column;
import dataframe.DataFrame;
import dataframe.exceptions.CustomException;
import dataframe.groupby.Applyable;
import dataframe.value.ValDouble;
import dataframe.value.Value;

import java.util.*;

public class Mediana implements Applyable {
    @Override
    public DataFrame apply(DataFrame dataFrame) throws CustomException {
        //sort each columns on the go and return middle value
        if (dataFrame.size()==0) throw new IllegalStateException("Dataframe of size 0!");
        DataFrame output = new DataFrame(dataFrame.columns,dataFrame.types);
        Value[] values = new Value[output.dataframe.size()];
        int index = 0;
        for (Column column: dataFrame.dataframe){
            List<Value> valuesOfColumn = new ArrayList<>(column.getArrayList());
            Collections.sort(valuesOfColumn);
            int size = valuesOfColumn.size();
            if (size%2!=0)
                values[index++] = valuesOfColumn.get((size-1)/2);
            else{
                values[index++] = (valuesOfColumn.get((size-2)/2).add(valuesOfColumn.get((size)/2))).mul(new ValDouble(0.5));}
        }
        output.add(values);
        return output;
    }
}
