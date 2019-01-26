package dataframe;

import dataframe.exceptions.CustomException;
import dataframe.value.*;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Collections;

public class StdServer {
    static DataFrame.GroupByDataFrame group;
    static int port = 9002;

    public static DataFrame std() throws CustomException {
        DataFrame output = group.CreateDataFrameOfSpecifiedIndexes();
        DataFrame mean = group.mean();
        Value.ValueBuilder[] builders = new Value.ValueBuilder[output.dataframe.size()];
        for (int i = 0; i < builders.length; i++) builders[i] = Value.builder(output.types[i]);

        int currentDf = 0;
        IntHolder exp = new IntHolder(2);
        for (DataFrame dataFrame : group.groupDataFrameList) {
//                System.out.println("wejscie do wątku "+dataFrame.getDataframe().get(1).getArrayList().get(0));

            int index = 0;
            int currentCol = 0;
            Value[] currentRow = new Value[output.dataframe.size()];
            for (Column column : dataFrame.dataframe) {
                if (group.groupedCols.contains(currentCol)) {
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
//                        System.out.println(sum);
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

    public static void main(String[] args) throws Exception {
        ServerSocket serverSocket = null;
        try {
            serverSocket = new ServerSocket(9002);
        } catch (IOException e) {
            System.out.println("Could not listen on port: 9002");
            System.exit(-1);
        }

        Socket clientSocket = null;
        try {
            clientSocket = serverSocket.accept();
        } catch (IOException e) {
            System.out.println("Accept failed: 9002");
            System.exit(-1);
        }
        /*for (int i=0; i<5; i++){
            list.add(new MaxServer()); //inicjalizacja serwera
            System.out.println(getSockets().get(i).getPort());
        }
        for (int i=0; i<5; i++){
            DataFrame df = (DataFrame)getInputStreams().get(i).readObject();
            df.print();
        }*/

        ObjectInputStream in = new ObjectInputStream(clientSocket.getInputStream());
        ObjectOutputStream out = new ObjectOutputStream(clientSocket.getOutputStream());
        //PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
           /* BufferedReader in = new BufferedReader(
                    new InputStreamReader(
                            clientSocket.getInputStream()));*/
        long timeofstart = System.currentTimeMillis();
        while (System.currentTimeMillis()-timeofstart<10000000){
            Object object = null;
            try {
                object = in.readObject();
            }
            catch (Exception e){        out.close();
                in.close();
                clientSocket.close();
                serverSocket.close();}
//               Object
            String inputLine;

            /*while ((inputLine = in.readLine()) != null) {
                out.println(inputLine);
            }*/
            if (object!=null){
                group = (DataFrame.GroupByDataFrame)object;
                //group.groupDataFrameList.get(0).print();

//                    min().print();

                //df.print();

                out.writeObject(std());}
        }



        out.close();
        in.close();
        clientSocket.close();
        serverSocket.close();

    }
}
