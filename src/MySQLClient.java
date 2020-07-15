
import java.io.*;
import java.sql.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MySQLClient {
	private Connection connect = null;
    private Statement stmt = null;
    static final String dbName = "netanelTargil3";

    // DB Information
    static final String DB_URL = "jdbc:mysql://localhost/";
    static final String USER = "root";
    static final String PASS = "Aa123456";



    public void createSchema() {  /**/ 
        try {
            connect = DriverManager.getConnection(DB_URL+"?verifyServerCertificate=false&useSSL=false&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC", USER, PASS);
            System.out.println("Creating database");
            stmt = connect.createStatement();
            String sql = "CREATE DATABASE IF NOT EXISTS " + dbName;
            stmt.executeUpdate(sql);
            stmt.executeUpdate("USE "+dbName);
            createTables();
            System.out.println("Database created successfully...");

           
        } catch (SQLException se) {
            se.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (stmt != null)
                    stmt.close();
            } catch (SQLException se2) {
            }// nothing to do
            try {
                if (connect != null)
                    connect.close();
            } catch (SQLException se) {
                se.printStackTrace();
            }
        }
    }

    public void loadDataToTables() {
    	
        String personsFile = "//Users//netanelaboutboul//Desktop//datafiles//personData.csv";
        String carsFile = "//Users//netanelaboutboul//Desktop//datafiles//carData.csv";
        String marriageFile = "//Users//netanelaboutboul//Desktop//datafiles//marriageData.csv";
        String carPeopleFile = "//Users//netanelaboutboul//Desktop//datafiles//carPeaple.csv";
        
        System.out.println("Loading persons data");
        loadDataToTable("persons", personsFile);
        System.out.println("Done! \nLoading relations data");
        loadDataToTable("relations", marriageFile);
        System.out.println("Done! \nLoading cars data");
        loadDataToTable("cars", carsFile);
        System.out.println("Done! \nLoading carsownedbypeople data");
        loadDataToTable("carsownedbypeople", carPeopleFile);
        System.out.println("Done! \n");
        
    }

    public void loadDataToTable(String tableName, String fileName){ 
        Statement stmt = null;
        Connection conn = null;
        try {
            conn = useDatabase(USER, PASS);

            stmt = conn.createStatement();

            FileInputStream fstream = new FileInputStream(fileName);
            DataInputStream in = new DataInputStream(fstream);
            BufferedReader br = new BufferedReader(new InputStreamReader(in));

            String strLine;

            while ((strLine = br.readLine()) != null) {

                String result = "INSERT INTO " + tableName + " VALUES(";
                String[] splitSt = strLine.split(",");
                for (String str : splitSt) {
                    str = str.replaceAll("^\\s+", "").replaceAll("\\s+$", "");
                    result = result + getRightFormat(str) + ",";
                }
                result = result.substring(0, result.length() - 1) + ")";

                try {
                    stmt.executeUpdate(result);
                } catch (Exception e) {
                    System.err.println("Error! Exception!");
                    System.err.println(e.getMessage());

                }
            }
            br.close();
        } catch (SQLException se) {
            se.printStackTrace();
        } catch (Exception e) {
            System.out.println("Failed");
        }
    }
    

    public void dropSchema() { 
        try {
            connect = DriverManager.getConnection(DB_URL+"?verifyServerCertificate=false&useSSL=false", USER, PASS);
        } catch (Exception e) {
            System.out.println("Connection to Database Failed");
        }


        try {
        stmt = connect.createStatement();
        System.out.println("Dropping database");
        String sql = "DROP DATABASE IF EXISTS "+ dbName;
        stmt.executeUpdate(sql);
        System.out.println("Done!");
        }
        catch (SQLException se) {
        	se.printStackTrace();
        	System.out.println("Failed");
        }
        finally {
            try {
                if (stmt != null)
                    stmt.close();
            } catch (SQLException se2) {
            }// nothing to do
            try {
                if (connect != null)
                    connect.close();
            } catch (SQLException se) {
                se.printStackTrace();
            }
        }
    }

    
    public void showTable(String tableName) { 
    	 try {
             connect = DriverManager.getConnection(DB_URL+"?verifyServerCertificate=false&useSSL=false", USER, PASS);
             System.out.println("Showing tables :");
             stmt = connect.createStatement();
             System.out.println(tableName + " table contects :\n");
             ResultSet rs = stmt.executeQuery("SELECT * FROM "+dbName+"."+tableName);
             ResultSetMetaData rsmd = rs.getMetaData();
             int ColumnSize = rsmd.getColumnCount();
             String strToPrint;
             while (rs.next()) {
            	 for(int i=1 ; i<=ColumnSize;i++)
            	 {
            		 strToPrint = rs.getString(i);
               	  System.out.print(strToPrint + " , "); 
            	 }
            	  System.out.println();
            	}
         } catch (SQLException se) {
        	 System.out.println(">>>>>> A SQL ERROR !!! <<<<<<");
             se.printStackTrace();
         } catch (Exception e) {
             e.printStackTrace();
         } finally {
             try {
                 if (stmt != null)
                     stmt.close();
             } catch (SQLException se2) {
             }// nothing to do
             try {
                 if (connect != null)
                     connect.close();
             } catch (SQLException se) {
                 se.printStackTrace();
             }
         }

    }
    
    public String submitPredefinedQuery(int queryNumber) { 
        
        String sqlQueryMinChildren = "SELECT person_id from "+dbName+".relations group by person_id  having count(person_id) in (select min(childs) from (SELECT count(person_id) as childs from "+dbName+".relations where relationship='child' group by person_id) child) LIMIT 1;";
        
        String sqlQueryAvgChildren = "SELECT AVG(childrens) FROM (SELECT id,count(id) as childrens FROM "+dbName+".persons INNER JOIN "+dbName+".relations ON id=person_id where Sex='Male' AND relationship='child' group by id) as X; \r\n";
        
        String sqlQueryAvgChildrenMoreCar = "SELECT AVG(childrens) FROM (SELECT id,count(id) as childrens FROM "+dbName+".persons INNER JOIN "+dbName+".relations ON id=person_id AND relationship='child' where id not in (select person_id from "+dbName+".carsownedbypeople)  group by id) as X;";
        
        String sqlQueryNativeCountryOfRedVolvo=  "SELECT Native_country FROM "+dbName+".persons where id IN (SELECT person_id FROM "+dbName+".carsownedbypeople WHERE color='red' AND car_id IN (SELECT car_id FROM "+dbName+".cars WHERE car_manufacturer='VOLVO')) GROUP BY Native_country LIMIT 1;";
        
        String result="";

        switch (queryNumber){
            case 1: result=submitScalarQuery(sqlQueryMinChildren);
                break;
            case 2: result=submitScalarQuery(sqlQueryAvgChildren);
                break;
            case 3: result=submitScalarQuery(sqlQueryAvgChildrenMoreCar);
                break;
            case 4: result=submitScalarQuery(sqlQueryNativeCountryOfRedVolvo);
            	break;
            default:
            	result= "Their is no Predefined query for number " + queryNumber;
        }
        
        return result;
    }
    
    public void createReport(int reportNumber) { 
        String report1 = "SELECT  person_id,relative_id,relationship,Age,Workclass,Education,Education_num, Marital_status,Race,Sex,Capital_gain,Native_country  FROM "+dbName+".relations INNER JOIN "+dbName+".persons ON id=relative_id WHERE relationship='child' ORDER BY Age;";
        String report2 ="select person_id,car_id,moel_year FROM "+dbName+".carsownedbypeople WHERE person_id in (SELECT NoChildren.id FROM (SELECT id, count(car_id) as cont FROM "+dbName+".persons INNER JOIN "+dbName+".carsownedbypeople ON id=person_id Where Sex='Male' GROUP BY person_id HAVING cont>1) as Morecars INNER JOIN (SELECT * FROM "+dbName+".persons WHERE id NOT IN (SELECT person_id from "+dbName+".relations WHERE relationship='child')) as NoChildren ON Morecars.id=NoChildren.id) ORDER BY moel_year;";
        if(reportNumber==1)
        	printQueryResult(report1);
        else if (reportNumber==2)
        	printQueryResult(report2);
        else
        	System.out.println("Report number " + reportNumber + " doesn't exist");
        	
    }
    
    public void sqlQuery(String sqlQuery) { 
        printQueryResult(sqlQuery);
    }
    
    private String submitScalarQuery(String sqlQuery) { 
        Statement stmt = null;
        Connection conn = null;
        String result = "";
        try {
            conn = useDatabase(USER, PASS);
            stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(sqlQuery);
            rs.next();
            result += rs.getString(1);
        }
        catch(SQLException se){
            System.out.println("Error handling query");
        }
        catch (Exception e){
            System.out.println("Connection Failed");
        }
        return result;
    }


    private void createTables() throws SQLException { 
        createPersonsTable();
        createMarriedAndDescendants();
        createCars();
        createCarsOwnedByPeapleTable();      
    }

    private void createPersonsTable() throws SQLException { /*!*/
        String personsTable =
                "CREATE TABLE IF NOT EXISTS Persons" + "(id int(11) NOT NULL," + "Age int(11) DEFAULT NULL," +
        "Workclass enum('Private', 'Self-emp-not-inc', 'Self-emp-inc', 'Federal-gov', 'Local-gov', 'State-gov', 'Without-pay', 'Never-worked') DEFAULT NULL,"+
                		"Education enum('Bachelors','Some-college','11th','HS-grad','Prof-school','Assoc-acdm','Assoc-voc','9th','7th-8th','12th','Masters','1st-4th','10th','Doctorate','5th-6th','Preschool') DEFAULT NULL,"+
                		"Education_num int(11) DEFAULT NULL," + "Marital_status enum('Married-civ-spouse','Divorced','Never-married','Separated','Widowed','Married-spouse-absent','Married-AF-spouse') DEFAULT NULL,"+
                		"Race enum('White', 'Asian-Pac-Islander', 'Amer-Indian-Eskimo', 'Other', 'Black') DEFAULT NULL,"+"Sex enum('Female','Male') DEFAULT NULL,"+ "Capital_gain int(11) DEFAULT NULL,"+
                		"Native_country enum('United-States','Cambodia','England','Puerto-Rico','Canada','Germany','Outlying-US(Guam-USVI-etc)','India','Japan','Greece','South','China','Cuba','Iran','Honduras','Philippines','Italy','Poland','Jamaica','Vietnam','Mexico','Portugal','Ireland','France','Dominican-Republic','Laos','Ecuador','Taiwan','Haiti','Columbia','Hungary','Guatemala','Nicaragua','Scotland','Thailand','Yugoslavia','El-Salvador','Trinadad&Tobago','Peru','Hong','Holand-Netherlands') DEFAULT NULL,"
                        +"PRIMARY KEY (id))";


        stmt.executeUpdate(personsTable);
    }
    
    private void createMarriedAndDescendants() throws SQLException{ /*!*/
    	String relations="CREATE TABLE IF NOT EXISTS Relations"+"(person_id int(11) NOT NULL,"+"relative_id int(11) NOT NULL,"+"relationship enum('wife','husband','child') DEFAULT NULL,\n"
    			+"PRIMARY KEY (person_id,relative_id),"+"FOREIGN KEY (person_id) REFERENCES persons(id))";
    	stmt.executeUpdate(relations);
    }
    
    private void createCars() throws SQLException{
    	String cars = " CREATE TABLE IF NOT EXISTS Cars"+"(car_id int(11) NOT NULL,"+"car_manufacturer varchar(255) NOT NULL,"+"car_model varchar(255) NOT NULL,"+"car_year int(11) NOT NULL, \n"
    			+ "PRIMARY KEY (car_id))";
    	stmt.execute(cars);
    }
    private void createCarsOwnedByPeapleTable() throws SQLException {
        String cars = "CREATE TABLE IF NOT EXISTS carsOwnedByPeople" + "(person_id int(11) NOT NULL," + "car_id int(11) NOT NULL," + "color varchar(10) NOT NULL," + "moel_year Date ,\n"
                + "PRIMARY KEY (car_id,person_id)," + "FOREIGN KEY (person_id) REFERENCES persons(id),"+"FOREIGN KEY (car_id) REFERENCES Cars(car_id))";
        stmt.executeUpdate(cars);
    }

    public void printQueryResult(String sql) {
        Connection conn;
        Statement stmt;
        String format="";
        try {
            conn = useDatabase(USER, PASS);

            stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(sql);
            String result="";
            int ncol = rs.getMetaData().getColumnCount();
            ResultSetMetaData rsmd = rs.getMetaData();
            
            result+=rsmd.getColumnName(1);
            
            for(int i= 2; i<=ncol; i++)
            {
            	format += "|%20s";
            	result += ","+rsmd.getColumnName(i);
            }
            format+="|%20s";
            
            System.out.println(String.format(format, result.substring(0).split(",",ncol)));
            	
            result ="";
            System.out.println();
            while (rs.next()) {
                for (int i = 1; i <= ncol; i++) {
                	result+=","+rs.getString(i);
                }
                
                System.out.println(String.format(format, result.substring(1).split(",",ncol)));
                result="";
            }
        } catch (SQLException se) {
            System.out.println(se);
        }
        catch (Exception e){
            System.out.println(e);
        }
    }
    
    public String returnQuery(int queryNum) {
        String result="";
        switch (queryNum) {
            case 1: result="Find the Person having the minium number of children ";
                break;
            case 2: result="Find the Average number of children of mans ";
                break;
            case 3: result="Find the Number of Phildren of persons without a car ";
                break;
            case 4: result="Find the Native country of maximum people with red Volvo ";
        }
        return result;
    }

    private Connection useDatabase(String USER, String PASS) throws Exception {
        Connection dbConnection;
        dbConnection = DriverManager.getConnection(DB_URL + dbName + "?verifyServerCertificate=false&useSSL=false", USER, PASS);
        return dbConnection;
    }
    
    private String getRightFormat(String str) { 
        String regex = "^[0-3]?[0-9]/[0-3]?[0-9]/(?:[0-9]{2})?[0-9]{2}$";
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(str);


        if (str.equals("?")) {
            str = "NULL";
        } else
        if (matcher.matches()) {

            String[] splitSt = str.split("/");
            str = "'" + splitSt[2] + "-" + splitSt[1] + "-" + splitSt[0] + "'";


        } else if (!isInteger(str)) {
            str = "'" + str + "'";
        }
        return str;
    }

    private boolean isInteger(String str) { 
        boolean result=true;

        if (str == null) {
            result= false;
        }
        int length = str.length();
        if (length == 0) {
            result=  false;
        }
        int i = 0;
        if (str.charAt(0) == '-') {
            if (length == 1) {
                result= false;
            }
            i = 1;
        }
        for (; i < length; i++) {
            char c = str.charAt(i);
            if (c < '0' || c > '9') {
                result=  false;
            }
        }

        return result;

    }

}