package dataframe.groupby;

import dataframe.DataFrame;
import dataframe.exceptions.CustomException;

public interface Applyable {
    DataFrame apply(DataFrame dataFrame) throws CustomException;
}
