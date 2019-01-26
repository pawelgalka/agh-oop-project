package dataframe;

import dataframe.value.Value;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;

public class MaxServer {
    static DataFrame.GroupByDataFrame group;
    static int port = 9001;

    public static DataFrame max() {

        DataFrame output = new DataFrame(group.columns, group.types);

        for (DataFrame dataFrame : group.groupDataFrameList) {
            int index = 0;
            int currentCol = 0;

            Value[] currentRow = new Value[group.columns.length];
            for (Column column : dataFrame.dataframe) {
                if (group.groupedCols.contains(currentCol++)) {
                    currentRow[index++] = column.getArrayList().get(0);
                } else {
                    currentRow[index++] = Collections.max(column.getArrayList());
                }
            }
            output.add(currentRow.clone());

        }
        return output;
    }

    public static void main(String[] args) throws Exception {
//        ArrayList<MaxServer> list = new ArrayList<>();
        ServerSocket serverSocket = null;
        try {
            serverSocket = new ServerSocket(9001);
        } catch (IOException e) {
            System.out.println("Could not listen on port: 9001");
            System.exit(-1);
        }

        Socket clientSocket = null;
        try {
            clientSocket = serverSocket.accept();
        } catch (IOException e) {
            System.out.println("Accept failed: 9001");
            System.exit(-1);
        }


        ObjectInputStream in = new ObjectInputStream(clientSocket.getInputStream());
        ObjectOutputStream out = new ObjectOutputStream(clientSocket.getOutputStream());

           long timeofstart = System.currentTimeMillis();
           while (System.currentTimeMillis()-timeofstart<10000000){
               Object object = null;
               try {
                   object = in.readObject();
               }
               catch (Exception e){        /*out.close();
                   in.close();
                   clientSocket.close();
                   serverSocket.close();
               return;*/}

            if (object!=null){
               group = (DataFrame.GroupByDataFrame)object;
               //group.groupDataFrameList.get(0).print();

               max().print();

               //df.print();

               out.writeObject(max());}
//            else{continue;}
           }




        out.close();
        in.close();
        clientSocket.close();
        serverSocket.close();

    }
}
