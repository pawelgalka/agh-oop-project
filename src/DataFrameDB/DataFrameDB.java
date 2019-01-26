package DataFrameDB;

import java.sql.*;

import dataframe.exceptions.CustomException;
import dataframe.value.*;
import dataframe.DataFrame;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;

public class DataFrameDB extends DataFrame {
    public Connection conn = null;
    private Statement stmt = null;
    private ResultSet rs = null;
    private ResultSetMetaData rsmd = null;
    private String tableName;
    private String table;

    public DataFrameDB(){
        connect();
    }
    public DataFrameDB(String tableName){
        //super();
        this.tableName = tableName;
        connect();
        System.out.println("Connected successfully");
    }

    public DataFrameDB(String fileName, Class<? extends Value>[] types, boolean header, String tableName) {
        super(fileName, types, header);
        connect();
        this.tableName = tableName;
        DataFrame dataFrame = new DataFrame(fileName,types,header);
        this.types = types;
        columns = dataFrame.getColumns();
        System.out.println("Data frame done");
        createTable();
        insertDataFrame(dataFrame);

        System.out.println(this.tableName);
    }

    public DataFrameDB(DataFrame dataFrame, String tableName){
        super(dataFrame);
        this.tableName = tableName;
        connect();
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
        String sql = "INSERT INTO " + tableName + " VALUES ";
        for (int i=0; i< dataFrame.size(); i++){
            sql+=" (";
            for(int k=0; k<columns.length; k++) {
                sql += "?,";
            }
            sql = sql.substring(0,sql.length()-1);
            if (i!=dataFrame.size()-1) sql += "), ";
            else sql+=")";
        }
        System.out.println(sql);
        try {
            pstmt = conn.prepareStatement(sql);
            int k = 1;
            for (int i=0; i<dataFrame.size(); i++){
                Value[] row = dataFrame.getRecord(i);
//                System.out.println(sql);

            for (Value value : row) {
                pstmt.setObject(k, value.getValue());
                k++;
            }}

            pstmt.execute();

        } catch (SQLException e) {
            e.printStackTrace();
        }
        /*for (int i=0; i< dataFrame.size(); i++){
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

        }*/
    }



    @Override
    public GroupByDataFrameDf groupby(String... cols) throws Exception {
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
                if (rsmd.getColumnClassName(i+1) == "java.lang.Integer") types[i] = IntHolder.class;
                if (rsmd.getColumnClassName(i+1) == "java.lang.Double") types[i] = DoubleHolder.class;
                if (rsmd.getColumnClassName(i+1) == "java.lang.Float") types[i] = FloatHolder.class;
                if (rsmd.getColumnClassName(i+1) == "java.sql.Date") types[i] = DateTimeHolder.class;
                if (rsmd.getColumnClassName(i+1) == "java.lang.String") types[i] = StringHolder.class;
//                System.out.println(types[i]);
            }

            DataFrame df = new DataFrame(columns,types);
            ArrayList<Integer> integers = df.GetIndexesOfColumns(cols);
            GroupByDataFrameDf groupByDataFrame = new GroupByDataFrameDf(columns,types,integers);
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

    public class GroupByDataFrameDf extends DataFrame.GroupByDataFrame{
        @Override
        public LinkedList<DataFrame> getGroupDataFrameList() {
            return super.getGroupDataFrameList();
        }

        @Override
        public String[] getColumns() {
            return super.getColumns();
        }

        @Override
        public Class<? extends Value>[] getTypes() {
            return super.getTypes();
        }

        @Override
        public ArrayList<Integer> getGroupedCols() {
            return super.getGroupedCols();
        }

        @Override
        public ArrayList<String> getGroupedColsNames() {
            return super.getGroupedColsNames();
        }

        public GroupByDataFrameDf() {
            super();
        }

        public GroupByDataFrameDf(LinkedList<DataFrame> linkedList, String[] colnames, Class<? extends Value>[] coltypes, ArrayList<Integer> groupedCols) {
            super(linkedList, colnames, coltypes, groupedCols);
        }

        public GroupByDataFrameDf(String[] cols, Class[] types, ArrayList<Integer> groupedCols) {
            super(cols, types, groupedCols);
        }

        @Override
        public void addDF(DataFrame df) {
            super.addDF(df);
        }

        @Override
        public DataFrame max() {
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
                    if (rsmd.getColumnClassName(i+1) == "java.lang.Integer") types[i] = IntHolder.class;
                    if (rsmd.getColumnClassName(i+1) == "java.lang.Double") types[i] = DoubleHolder.class;
                    if (rsmd.getColumnClassName(i+1) == "java.lang.Float") types[i] = FloatHolder.class;
                    if (rsmd.getColumnClassName(i+1) == "java.sql.Date") types[i] = DateTimeHolder.class;
                    if (rsmd.getColumnClassName(i+1) == "java.lang.String") types[i] = StringHolder.class;
                }

                DataFrame df = new DataFrame(columns,types);
                ArrayList<Integer> integers = df.GetIndexesOfColumns(getColumns());

                ArrayList<Integer> gr = new ArrayList<>();
                for (Integer i : integers){
                    System.out.println(i);
                }
                for (int i = 0 ; i<count; i++){
                    if (!getGroupedCols().contains(i)){
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
                DataFrame df1 = DataFrameDB.this.query(sql1);
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

        @Override
        public DataFrame min() {
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
                    if (rsmd.getColumnClassName(i+1) == "java.lang.Integer") types[i] = IntHolder.class;
                    if (rsmd.getColumnClassName(i+1) == "java.lang.Double") types[i] = DoubleHolder.class;
                    if (rsmd.getColumnClassName(i+1) == "java.lang.Float") types[i] = FloatHolder.class;
                    if (rsmd.getColumnClassName(i+1) == "java.sql.Date") types[i] = DateTimeHolder.class;
                    if (rsmd.getColumnClassName(i+1) == "java.lang.String") types[i] = StringHolder.class;
                }

                DataFrame df = new DataFrame(columns,types);
                ArrayList<Integer> integers = df.GetIndexesOfColumns(getColumns());
                ArrayList<Integer> gr = new ArrayList<>();

                for (int i = 0 ; i<count; i++){
                    if (!getGroupedCols().contains(i)){
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
                DataFrame df1 = DataFrameDB.this.query(sql1);
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
            return null;
        }

        @Override
        public DataFrame mean() {
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
                    if (rsmd.getColumnClassName(i+1) == "java.lang.Integer") types[i] = IntHolder.class;
                    if (rsmd.getColumnClassName(i+1) == "java.lang.Double") types[i] = DoubleHolder.class;
                    if (rsmd.getColumnClassName(i+1) == "java.lang.Float") types[i] = FloatHolder.class;
                    if (rsmd.getColumnClassName(i+1) == "java.sql.Date") types[i] = DateTimeHolder.class;
                    if (rsmd.getColumnClassName(i+1) == "java.lang.String") types[i] = StringHolder.class;
//                System.out.println(types[i]);
                }

//                DataFrame df = new DataFrame(columns,types);
                DataFrame df = CreateDataFrameOfSpecifiedIndexes();
                ArrayList<String> namesOfColumns = new ArrayList<>(Arrays.asList(df.columns));

                ArrayList<Integer> gr = new ArrayList<>();

                for (int i = 0 ; i<count; i++){
                    if (!namesOfColumns.contains(getColumns()[i])) continue;
                    if (!getGroupedCols().contains(i)){
                        sql.append(" avg("+columns[i]+") ,");
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
                DataFrame df1 = DataFrameDB.this.query(sql1);
                return df1;
            }catch (SQLException ex){;
                ex.printStackTrace();

            }
            catch (Exception e){e.printStackTrace();}
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
            return null;
        }

        @Override
        public DataFrame sum() throws CustomException {
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
                    if (rsmd.getColumnClassName(i+1) == "java.lang.Integer") types[i] = IntHolder.class;
                    if (rsmd.getColumnClassName(i+1) == "java.lang.Double") types[i] = DoubleHolder.class;
                    if (rsmd.getColumnClassName(i+1) == "java.lang.Float") types[i] = FloatHolder.class;
                    if (rsmd.getColumnClassName(i+1) == "java.sql.Date") types[i] = DateTimeHolder.class;
                    if (rsmd.getColumnClassName(i+1) == "java.lang.String") types[i] = StringHolder.class;
                }

                DataFrame df = CreateDataFrameOfSpecifiedIndexes();
                ArrayList<String> namesOfColumns = new ArrayList<>(Arrays.asList(df.columns));
                ArrayList<Integer> gr = new ArrayList<>();

                for (int i = 0 ; i<count; i++){
                    if (!getGroupedCols().contains(i)){
                        if (!namesOfColumns.contains(getColumns()[i])) continue;
                        sql.append(" sum("+columns[i]+") ,");
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
                DataFrame df1 = DataFrameDB.this.query(sql1);
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

        @Override
        public DataFrame std() throws CustomException {
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
                    if (rsmd.getColumnClassName(i+1) == "java.lang.Integer") types[i] = IntHolder.class;
                    if (rsmd.getColumnClassName(i+1) == "java.lang.Double") types[i] = DoubleHolder.class;
                    if (rsmd.getColumnClassName(i+1) == "java.lang.Float") types[i] = FloatHolder.class;
                    if (rsmd.getColumnClassName(i+1) == "java.sql.Date") types[i] = DateTimeHolder.class;
                    if (rsmd.getColumnClassName(i+1) == "java.lang.String") types[i] = StringHolder.class;

                }

                DataFrame df = CreateDataFrameOfSpecifiedIndexes();
                ArrayList<String> namesOfColumns = new ArrayList<>(Arrays.asList(df.columns));
                ArrayList<Integer> gr = new ArrayList<>();

                for (int i = 0 ; i<count; i++){
                    if (!getGroupedCols().contains(i)){
                        if (!namesOfColumns.contains(getColumns()[i])) continue;
                        sql.append(" std("+columns[i]+") ,");
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
                DataFrame df1 = DataFrameDB.this.query(sql1);
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
            return null;
        }

        @Override
        public DataFrame var() throws CustomException {
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
                    if (rsmd.getColumnClassName(i+1) == "java.lang.Integer") types[i] = IntHolder.class;
                    if (rsmd.getColumnClassName(i+1) == "java.lang.Double") types[i] = DoubleHolder.class;
                    if (rsmd.getColumnClassName(i+1) == "java.lang.Float") types[i] = FloatHolder.class;
                    if (rsmd.getColumnClassName(i+1) == "java.sql.Date") types[i] = DateTimeHolder.class;
                    if (rsmd.getColumnClassName(i+1) == "java.lang.String") types[i] = StringHolder.class;
                }

                DataFrame df = CreateDataFrameOfSpecifiedIndexes();
                ArrayList<String> namesOfColumns = new ArrayList<>(Arrays.asList(df.columns));

                ArrayList<Integer> gr = new ArrayList<>();

                for (int i = 0 ; i<count; i++){
                    if (!getGroupedCols().contains(i)){
                        if (!namesOfColumns.contains(getColumns()[i])) continue;
                        sql.append(" variance("+columns[i]+") ,");
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
                DataFrame df1 = DataFrameDB.this.query(sql1);
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
            return null;
        }
    }
    /*****************************DATABASE OPERATIONS************************************************************/

    public boolean connect() {
        try {
            for (int i = 0; i < 3; i++) {
                Class.forName("com.mysql.jdbc.Driver").newInstance();
                conn = DriverManager.getConnection("jdbc:mysql://mysql.agh.edu.pl/pawelgal", "pawelgal", "jcfkxoDjgLoB63lk");
            }
        } catch (Exception ex)
        {
            ex.printStackTrace();
            return false;
        }

        if(conn!=null) return true;
        return false;
    }

    public void createTable(){

        try {
            System.out.println(conn);
            System.out.println(columns[0]);
            stmt = conn.createStatement();
            String a = "DROP TABLE IF EXISTS "+tableName+";";
            stmt.executeUpdate(a);
            stmt=conn.createStatement();
            String query = "CREATE TABLE " + tableName + " (";
            for (int i = 0; i < columns.length; i++) {
//                System.out.println(columns[i].length());
                if(columns[i].length()==0)columns[i]="NaN";
                if (types[i] == IntHolder.class) {
                    query += columns[i] + " INT ";
                } else if (types[i] == DoubleHolder.class) {
                    query += columns[i] + " DOUBLE ";
                } else if (types[i] == FloatHolder.class) {
                    query += columns[i] + " FLOAT ";
                } else if (types[i] == StringHolder.class) {
                    query += columns[i] + " VARCHAR(256) ";
                } else if (types[i] == ValBoolean.class) {
                    query += columns[i] + " BOOLEAN ";
                } else if (types[i] == DateTimeHolder.class) {
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
                types[i-1] = (IntHolder.class);
            }
            else if(metadata.getColumnTypeName(i).equals("DOUBLE")){
                types[i-1] = (DoubleHolder.class);
            }
            else if(metadata.getColumnTypeName(i).equals("FLOAT")){
                types[i-1] = (FloatHolder.class);
            }
            else if(metadata.getColumnTypeName(i).equals("CHAR") || metadata.getColumnTypeName(i).equals("VARCHAR")){
                types[i-1] = (StringHolder.class);
            }
            else if(metadata.getColumnTypeName(i).equals("DATE")){
                types[i-1] = (DateTimeHolder.class);
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
                if (rsmd.getColumnClassName(i+1) == "java.lang.Integer") types[i] = IntHolder.class;
                if (rsmd.getColumnClassName(i+1) == "java.lang.Double") types[i] = DoubleHolder.class;
                if (rsmd.getColumnClassName(i+1) == "java.lang.Float") types[i] = FloatHolder.class;
                if (rsmd.getColumnClassName(i+1) == "java.sql.Date") types[i] = DateTimeHolder.class;
                if (rsmd.getColumnClassName(i+1) == "java.lang.String") types[i] = StringHolder.class;
//                System.out.println(types[i]);
            }

            DataFrame df = new DataFrame(columns,types);

            while(rs.next()){
                Value[] values = new Value[rsmd.getColumnCount()];
                for (int i=0; i<rsmd.getColumnCount(); ++i){
                    values[i] = Value.builder(types[i]).build(rs.getString(i+1));
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
