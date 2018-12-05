package DataFrameDB;

import java.sql.*;
import dataframe.value.*;
import dataframe.exceptions.*;
import dataframe.DataFrame;
import java.util.ArrayList;

public class DataFrameDB extends DataFrame {
    private Connection conn = null;
    private Statement stmt = null;
    private ResultSet rs = null;
    private ResultSetMetaData rsmd = null;
    private String tableName;
    private String table;

    public DataFrameDB(String tableName){
        //super();
        this.tableName = tableName;
        connect();
        System.out.println("Connected successfully");
    }

    public DataFrameDB(String fileName, Class<? extends Value>[] types, boolean header, String tableName) {
        //super(fileName, types, header);
        connect();
        this.tableName = tableName;
        DataFrame dataFrame = new DataFrame(fileName,types,header);
        this.types = types;
        columns = dataFrame.getColumns();
        createTable();
        insertDataFrame(dataFrame);

        System.out.println(this.tableName);
    }

    public DataFrameDB(DataFrame dataFrame, String tableName){
        this.tableName = tableName;
        connect();
        this.types = dataFrame.getTypes();
        columns = dataFrame.getColumns();
        createTable();
        insertDataFrame(dataFrame);
    }

    public void exit() {
        dropTable();
    }

    @Override
    public void print() {
        displayAll();
    }

    private void insertDataFrame(DataFrame dataFrame){
        PreparedStatement pstmt = null;
        for (int i=0; i< dataFrame.size(); i++){
            Value[] row = dataFrame.getRecord(i);

            String sql = "INSERT INTO " + tableName + " VALUES (";
            for(int k=0; k<columns.length; k++) {
                sql += "?,";
            }
            sql = sql.substring(0,sql.length()-1);
            sql += ")";

            try {
                pstmt = conn.prepareStatement(sql);


                int k = 1;
                for (Value value : row) {
                    pstmt.setObject(k, value.getValue());
                    k++;
                }
                pstmt.execute();

            } catch (SQLException e) {
                e.printStackTrace();
            }

        }
    }

    public DataFrame max(String... cols){
        StringBuilder sql = new StringBuilder("SELECT ");
        try {
            stmt = conn.createStatement();

            String query = "SELECT * FROM " + tableName+ " WHERE 1;";
            rs = stmt.executeQuery(query);
            rsmd = rs.getMetaData();
            int count = rsmd.getColumnCount();
            String[] columns = new String[rsmd.getColumnCount()];
            Class[] types = new Class[rsmd.getColumnCount()];
            for (int i=0; i<rsmd.getColumnCount(); i++){
                columns[i] = rsmd.getColumnName(i+1);
                if (rsmd.getColumnClassName(i+1) == "java.lang.Integer") types[i] = ValInteger.class;
                if (rsmd.getColumnClassName(i+1) == "java.lang.Double") types[i] = ValDouble.class;
                if (rsmd.getColumnClassName(i+1) == "java.lang.Float") types[i] = ValFloat.class;
                if (rsmd.getColumnClassName(i+1) == "java.sql.Date") types[i] = ValDateTime.class;
                if (rsmd.getColumnClassName(i+1) == "java.lang.String") types[i] = ValString.class;
//                System.out.println(types[i]);
            }

            DataFrame df = new DataFrame(columns,types);
            ArrayList<Integer> integers = df.GetIndexesOfColumns(cols);
            ArrayList<Integer> gr = new ArrayList<>();
            for (Integer i : integers){
                System.out.println(i);
            }
            for (int i = 0 ; i<count; i++){
                if (integers.contains(i)){
                    sql.append(" max("+columns[i]+") ,");
                }
                else {
                    sql.append(" "+columns[i]+" ,");
                    gr.add(i);
                }
            }
            String sql1 = sql.substring(0, sql.length()-1);
            sql1 += ("FROM "+tableName+" GROUP BY ");
            for (Integer i : gr){
                sql1 += columns[i]+" ,";
            }
            sql1 = sql1.substring(0,sql1.length()-1);
            sql1 = sql1 + ";";
            System.out.println(sql1);
            DataFrame df1 = this.query(sql1);
            return df1;
        }catch (SQLException ex){;
            ex.printStackTrace();

        }
        catch (Exception e){}
        finally {
            // zwalniamy zasoby, które nie będą potrzebne
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException sqlEx) { } // ignore
                rs = null;
            }

            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException sqlEx) { } // ignore

                stmt = null;
            }
        }
//        return this.query(query);
        return null;
    }

    public DataFrame min(String... cols){
        StringBuilder sql = new StringBuilder("SELECT ");
        try {
            stmt = conn.createStatement();

            String query = "SELECT * FROM " + tableName+ " WHERE 1;";
            rs = stmt.executeQuery(query);
            rsmd = rs.getMetaData();
            int count = rsmd.getColumnCount();
            String[] columns = new String[rsmd.getColumnCount()];
            Class[] types = new Class[rsmd.getColumnCount()];
            for (int i=0; i<rsmd.getColumnCount(); i++){
                columns[i] = rsmd.getColumnName(i+1);
                if (rsmd.getColumnClassName(i+1) == "java.lang.Integer") types[i] = ValInteger.class;
                if (rsmd.getColumnClassName(i+1) == "java.lang.Double") types[i] = ValDouble.class;
                if (rsmd.getColumnClassName(i+1) == "java.lang.Float") types[i] = ValFloat.class;
                if (rsmd.getColumnClassName(i+1) == "java.sql.Date") types[i] = ValDateTime.class;
                if (rsmd.getColumnClassName(i+1) == "java.lang.String") types[i] = ValString.class;
//                System.out.println(types[i]);
            }

            DataFrame df = new DataFrame(columns,types);
            ArrayList<Integer> integers = df.GetIndexesOfColumns(cols);
            ArrayList<Integer> gr = new ArrayList<>();
            for (Integer i : integers){
                System.out.println(i);
            }
            for (int i = 0 ; i<count; i++){
                if (integers.contains(i)){
                    sql.append(" min("+columns[i]+") ,");
                }
                else {
                    sql.append(" "+columns[i]+" ,");
                    gr.add(i);
                }
            }
            String sql1 = sql.substring(0, sql.length()-1);
            sql1 += ("FROM "+tableName+" GROUP BY ");
            for (Integer i : gr){
                sql1 += columns[i]+" ,";
            }
            sql1 = sql1.substring(0,sql1.length()-1);
            sql1 = sql1 + ";";
            System.out.println(sql1);
            DataFrame df1 = this.query(sql1);
            return df1;
        }catch (SQLException ex){;
            ex.printStackTrace();

        }
        catch (Exception e){}
        finally {
            // zwalniamy zasoby, które nie będą potrzebne
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException sqlEx) { } // ignore
                rs = null;
            }

            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException sqlEx) { } // ignore

                stmt = null;
            }
        }
//        return this.query(query);
        return null;
    }

    public DataFrame.GroupByDataFrame groupBy(String... cols){
        try {
            stmt = conn.createStatement();

            String query = "SELECT * FROM " + tableName+ " WHERE 1;";
            rs = stmt.executeQuery(query);
            rsmd = rs.getMetaData();
            int count = rsmd.getColumnCount();
            String[] columns = new String[rsmd.getColumnCount()];
            Class[] types = new Class[rsmd.getColumnCount()];
            for (int i=0; i<rsmd.getColumnCount(); i++){
                columns[i] = rsmd.getColumnName(i+1);
                if (rsmd.getColumnClassName(i+1) == "java.lang.Integer") types[i] = ValInteger.class;
                if (rsmd.getColumnClassName(i+1) == "java.lang.Double") types[i] = ValDouble.class;
                if (rsmd.getColumnClassName(i+1) == "java.lang.Float") types[i] = ValFloat.class;
                if (rsmd.getColumnClassName(i+1) == "java.sql.Date") types[i] = ValDateTime.class;
                if (rsmd.getColumnClassName(i+1) == "java.lang.String") types[i] = ValString.class;
//                System.out.println(types[i]);
            }

            DataFrame df = new DataFrame(columns,types);
            ArrayList<Integer> integers = df.GetIndexesOfColumns(cols);
            DataFrame.GroupByDataFrame groupByDataFrame = new DataFrame.GroupByDataFrame(columns,types,integers);
            ArrayList<Integer> gr = new ArrayList<>();
            StringBuilder zapytanie = new StringBuilder("SELECT DISTINCT ");
            for (int i = 0 ; i<count; i++){
                if (integers.contains(i)){
                    zapytanie.append(" "+columns[i]+" ,");
                }
                else {
                    gr.add(i);
                }
            }
            String sql1 = zapytanie.substring(0, zapytanie.length()-1);
            sql1 += ("FROM "+tableName);
            PreparedStatement preparedStatement = conn.prepareStatement(sql1);
            rs = preparedStatement.executeQuery();
            rsmd = rs.getMetaData();
            int k =1;
            ArrayList<String> queries = new ArrayList<>();
            while (rs.next()){
                //System.out.println(rs.getString(1));
                StringBuilder query1 = new StringBuilder("SELECT * from "+tableName+" WHERE ");

                int length = rsmd.getColumnCount();
                for (int i=1; i<=length; i++){
                    query1.append(rsmd.getColumnName(i)+"='"+rs.getString(i)+"' AND ");
                }
                String sql2 = query1.substring(0,query1.length()-4);
                queries.add(sql2);
            }
            for(String str:queries){
//                DataFrame dataFrame = query(str);
                groupByDataFrame.addDF(query(str));
            }
            return groupByDataFrame;
        }catch (SQLException ex){;
            ex.printStackTrace();

        }
        catch (Exception e){}
        finally {
            // zwalniamy zasoby, które nie będą potrzebne
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException sqlEx) { } // ignore
                rs = null;
            }

            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException sqlEx) { } // ignore

                stmt = null;
            }
        }
//        return this.query(query);
        return null;
    }
    /*****************************DATABASE OPERATIONS************************************************************/


    public void connect(){
        try {
            Class.forName("com.mysql.jdbc.Driver").newInstance();
            conn = DriverManager.getConnection("jdbc:mysql://mysql.agh.edu.pl/pawelgal", "pawelgal", "gD7GTUijLdt4hyXi");

        } catch (SQLException ex) {
            // handle any errors
            System.out.println("SQLException: " + ex.getMessage());
            System.out.println("SQLState: " + ex.getSQLState());
            System.out.println("VendorError: " + ex.getErrorCode());
        }catch(Exception e){e.printStackTrace();}
    }

    public void createTable(){

        try {
            System.out.println(conn);
            System.out.println(columns[0]);
            stmt = conn.createStatement();
            String query = "CREATE TABLE " + tableName + " (";
            for (int i = 0; i < columns.length; i++) {
                if (types[i] == ValInteger.class) {
                    query += columns[i] + " INT ";
                } else if (types[i] == ValDouble.class) {
                    query += columns[i] + " DOUBLE ";
                } else if (types[i] == ValFloat.class) {
                    query += columns[i] + " FLOAT ";
                } else if (types[i] == ValString.class) {
                    query += columns[i] + " VARCHAR(256) ";
                } else if (types[i] == ValBoolean.class) {
                    query += columns[i] + " BOOLEAN ";
                } else if (types[i] == ValDateTime.class) {
                    query += columns[i] + " DATE ";
                } else {
                    query += columns[i] + " NULL ";
                }
                query += ", ";
            }
            query = query.substring(0,query.length()-2);
            query += " ) ";
            System.out.println(query);
            stmt.executeUpdate(query);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void dropTable() {
        try {
            stmt=conn.createStatement();
            String query="DROP TABLE " + tableName;
            stmt.executeUpdate(query);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void displayAll() {
        try {
            stmt = conn.createStatement();

            String query = "SELECT * FROM " + tableName;
            rs = stmt.executeQuery(query);
            rsmd = rs.getMetaData();
            int count = rsmd.getColumnCount();

            String name = "";
            for (int i = 1; i < count + 1; i++) {
                if (i == count) {
                    name += rsmd.getColumnName(i);
                } else {
                    name += rsmd.getColumnName(i) + " | ";
                }
            }
            name += "\n";

            while(rs.next()){

                for (int i = 1; i < count + 1; i++) {
                    if (i == count) {
                        name += rs.getString(i);
                    } else {
                        name += rs.getString(i) + " | ";
                    }
                }
                name += "\n";
            }
            System.out.println(name);
        }catch (SQLException ex){;
            ex.printStackTrace();

        }finally {
            // zwalniamy zasoby, które nie będą potrzebne
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException sqlEx) { } // ignore
                rs = null;
            }

            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException sqlEx) { } // ignore

                stmt = null;
            }
        }
    }

    private void catchNamesAndTypes(ResultSet resultSet) throws SQLException {
        ResultSetMetaData metadata = resultSet.getMetaData();
        int columnCount = metadata.getColumnCount();
        columns = new String[columnCount];
        types = new Class[columnCount];
        for (int i = 1; i <= columnCount; i++) {
            String columnName = metadata.getColumnName(i);
            columns[i-1] = (columnName);

            if(metadata.getColumnTypeName(i).equals("INT") || metadata.getColumnTypeName(i).equals("SMALLINT") || metadata.getColumnTypeName(i).equals("BIGINT") || metadata.getColumnTypeName(i).equals("MEDIUMINT") || metadata.getColumnTypeName(i).equals("TINYINT")){
                types[i-1] = (ValInteger.class);
            }
            else if(metadata.getColumnTypeName(i).equals("DOUBLE")){
                types[i-1] = (ValDouble.class);
            }
            else if(metadata.getColumnTypeName(i).equals("FLOAT")){
                types[i-1] = (ValFloat.class);
            }
            else if(metadata.getColumnTypeName(i).equals("CHAR") || metadata.getColumnTypeName(i).equals("VARCHAR")){
                types[i-1] = (ValString.class);
            }
            else if(metadata.getColumnTypeName(i).equals("DATE")){
                types[i-1] = (ValDateTime.class);
            }
        }
    }

    public DataFrame query(String query) {
        try {
            stmt = conn.createStatement();

            rs = stmt.executeQuery(query);
            rsmd = rs.getMetaData();
//            System.out.println(rsmd.getColumnCount());
            int count;
            String[] columns = new String[rsmd.getColumnCount()];
            Class[] types = new Class[rsmd.getColumnCount()];
            for (int i=0; i<rsmd.getColumnCount(); i++){
                columns[i] = rsmd.getColumnName(i+1);
                if (rsmd.getColumnClassName(i+1) == "java.lang.Integer") types[i] = ValInteger.class;
                if (rsmd.getColumnClassName(i+1) == "java.lang.Double") types[i] = ValDouble.class;
                if (rsmd.getColumnClassName(i+1) == "java.lang.Float") types[i] = ValFloat.class;
                if (rsmd.getColumnClassName(i+1) == "java.sql.Date") types[i] = ValDateTime.class;
                if (rsmd.getColumnClassName(i+1) == "java.lang.String") types[i] = ValString.class;
//                System.out.println(types[i]);
            }

            DataFrame df = new DataFrame(columns,types);

            while(rs.next()){
                Value[] values = new Value[rsmd.getColumnCount()];
                for (int i=0; i<rsmd.getColumnCount(); ++i){
                    if (types[i] == ValInteger.class){
                        values[i] = ValInteger.getInstance().create(rs.getString(i+1));
                    }
                    if (types[i] == ValDouble.class){
                        values[i] = ValDouble.getInstance().create(rs.getString(i+1));
                    }
                    if (types[i] == ValBoolean.class){
                        values[i] = ValBoolean.getInstance().create(rs.getString(i+1));
                    }
                    if (types[i] == ValFloat.class){
                        values[i] = ValFloat.getInstance().create(rs.getString(i+1));
                    }
                    if (types[i] == ValString.class){
                        values[i] = ValString.getInstance().create(rs.getString(i+1));
                    }
                    if (types[i] == ValDateTime.class){
                        values[i] = ValDateTime.getInstance().create(rs.getString(i+1));
                    }
                }
                df.add(values.clone());
            }
            return df;

        }catch (SQLException ex){
            ex.printStackTrace();

        }finally {
            // zwalniamy zasoby, które nie będą potrzebne
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException sqlEx) { } // ignore
                rs = null;
            }

            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException sqlEx) { } // ignore

                stmt = null;
            }
        }
        return null;
    }

    public DataFrame toDF(){
        DataFrame dataFrame = query("SELECT * FROM "+tableName);
        return dataFrame;
    }
    public boolean open(String table) {
        try {
            // polaczenie z baza
            //uzyskanie naglowkow i typow
            String str = "SELECT * FROM " + tableName;
            System.out.println(conn);
            PreparedStatement statement = conn.prepareStatement(str);
            ResultSet resultSet = statement.executeQuery();

            catchNamesAndTypes(resultSet);

            return true;
        } catch (SQLException e) {
            System.out.println("Couldn't connect to database: " + e.getMessage());
            return false;
        }

    }

    public void close() {
        try {
            if(conn != null) {
                conn.close();
            }
        } catch(SQLException e) {
            System.out.println("Couldn't close connection: " + e.getMessage());
        }
    }
}
