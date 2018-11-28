package dataframe.exceptions;

import dataframe.exceptions.CustomException;

public class InvalidTypeOperation extends CustomException {
    public InvalidTypeOperation(String msg, int row, String colname){
        super(msg+" column: "+colname+" row: "+row);
    }
}
