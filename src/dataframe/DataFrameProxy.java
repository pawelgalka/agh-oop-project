package dataframe;

import javafx.scene.Parent;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.LinkedList;

public class DataFrameProxy {
        private static ArrayList<Socket> sockets = new ArrayList<>();
        private static ArrayList<ObjectOutputStream> outputStreams = new ArrayList<>();
        private static ArrayList<ObjectInputStream> inputStreams = new ArrayList<>();
        public static void main(String[] args) throws Exception {
            ServerSocket serverSocket = null;
            Socket proxySocket = null;
            int outPort = -1;
            DataFrame.GroupByDataFrame df = null;
//            DataFrame df ;
            try {
                serverSocket = new ServerSocket(9000);
            } catch (IOException e) {
                System.out.println("Could not listen on port: 9000");
                System.exit(-1);
            }

            Socket clientSocket = null;
            try {
                clientSocket = serverSocket.accept();
            } catch (IOException e) {
                System.out.println("Accept failed: 9000");
                System.exit(-1);
            }
            //PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
           /* BufferedReader in = new BufferedReader(
                    new InputStreamReader(
                            clientSocket.getInputStream()));*/
            ObjectInputStream in = new ObjectInputStream(clientSocket.getInputStream());
            ObjectOutputStream out = new ObjectOutputStream(clientSocket.getOutputStream());
            long time = System.currentTimeMillis();
            while(System.currentTimeMillis()-time<10000000){
//                Object object = in.readObject();
               /* in.
                out = new ObjectOutputStream(clientSocket.getOutputStream());*/
                Object object = null;
                try {
                    object = in.readObject();

                }
                catch (Exception e){ /*out.close();
                    in.close();
                    clientSocket.close();
                    serverSocket.close(); return;*/}
//                String inputLine;

            /*while ((inputLine = in.readLine()) != null) {
                out.println(inputLine);
            }*/
            int port=-1;
            if (object!=null){
//                String type = (String)((LinkedList<Object>)object).get(1);
                Pocket p = (Pocket)object;
                String type = p.s;

                switch (type){
                    case "max":
                        port = 9001;
                        break;
                    case "min":
                        port = 9002;
                        break;
                    case "sum":
                        port = 9003;
                        break;
                    case "mean":
                        port = 9004;
                        break;
                    case "std":
                        port = 9005;
                        break;
                    case "var":
                        port = 9006;
                        break;
                    default:
                        port=0;
                }
                df = p.dataFrame;
//                111outPort = (int)((LinkedList<Object>)object).get(1);
//                System.out.println(outPort);
//               111df = (DataFrame.GroupByDataFrame)((LinkedList<Object>)object).get(0);
//                df = (DataFrame)((LinkedList<Object>)object).get(0);
//                df.print();
//                int port = 9001;
//                sockets.clear();
//                int size = df.size();
//                int from = 0;
//                for (int i=0; i<5; i++){
//                    int to = (i+1)*size/5;
//                    System.out.println("To "+to);
//                    sockets.add(new Socket("localhost",port++));
//                    System.out.println(sockets.get(i).getPort());
//                    outputStreams.add(new ObjectOutputStream(sockets.get(i).getOutputStream()));
//                    inputStreams.add(new ObjectInputStream(sockets.get(i).getInputStream()));
//                    outputStreams.get(i).writeObject(df.iloc(from,to));
//                    from = to;


//                DataFrame output;

                proxySocket = new Socket("localhost",port);
                ObjectOutputStream outToServerCounting = new ObjectOutputStream(proxySocket.getOutputStream());
                outToServerCounting.writeObject(df);
                ObjectInputStream inFromServerCounting = new ObjectInputStream(proxySocket.getInputStream());
                System.out.println(outPort);
//                df.groupDataFrameList.get(0).print();

                out.writeObject(inFromServerCounting.readObject());}
//                else{continue;}
//                out.writeObject(new DataFrame());
            }
//                if(object == null) break;
//            }

            out.close();
            in.close();
            clientSocket.close();
            serverSocket.close();

        }

}

class Pocket1 implements Serializable{

}