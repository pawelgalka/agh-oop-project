package dataframe;

public class MaxServer1 implements Runnable{
    DataFrame dataFrametoWork;
    int port;
    String type;
    MaxServer1(DataFrame df,int port,String type){
        dataFrametoWork = df;
        this.port = port;
        this.type = type;
    }
    @Override
    public void run() {


    }
}
