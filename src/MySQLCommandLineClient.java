 /**
 * Edited by Netanel Abutbul on 14/7/2020
 */
import java.util.Scanner;

public class MySQLCommandLineClient {

public static void main(String[] args) {
    MySQLClient mySQLClient = new MySQLClient();
    welcomeMenu();
    System.out.println("Please enter a legal command. Enter help for showing the commands:\n");

    Scanner sc = new Scanner(System.in);
    String command = sc.nextLine();

    String commandOp=command;
    String commandPar="";


    ExtractParameter extractParameter= new ExtractParameter(command).invoke();
    commandOp = extractParameter.getCommandOp();
    commandPar = extractParameter.getCommandPar();

    while (!command.equals("exit")) {
        switch (commandOp){
            case "help":
                showMainMenu();
                break;
            case "create schema":
                mySQLClient.createSchema();
                break;
            case "drop schema":
                mySQLClient.dropSchema();
                break;
            case "load all" :
                mySQLClient.loadDataToTables();
                break;
            case "load":
                String[] splitCommand = commandPar.split("\\s+");
                mySQLClient.loadDataToTable(splitCommand[1], splitCommand[0]);
                break;
            case "show":
               mySQLClient.showTable(commandPar);
                break;
            case "sql":
                mySQLClient.sqlQuery(commandPar);
                break;
            case "query":
                int queryNum=Integer.parseInt(commandPar);
                print(mySQLClient.returnQuery(queryNum) + ": "+
                        mySQLClient.submitPredefinedQuery(queryNum));
                break;
            case "report":
                int reportNum=Integer.parseInt(commandPar);
                mySQLClient.createReport(reportNum);
                break;
        }

        System.out.println("Please enter one of the possible commands\n");
        command = sc.nextLine();
        extractParameter= new ExtractParameter(command).invoke();
        commandOp = extractParameter.getCommandOp();
        commandPar = extractParameter.getCommandPar();

    }
}

    private static void print(String s) {
        System.out.println(s);
    }




    public static void welcomeMenu() {
    System.out.println("                Welcome to MYSQL Command Line ex3MySQL.MySQLClient");
    System.out.println("===========================================================\n");


}

public static void showMainMenu() {

    System.out.println("create schema" +
            "                           Creates the database schema (see database schema section)\n" +
            "drop schema" +
            "                             Deletes the schema (all tables, constraints, triggers etc)\n" +
            "load all" +
            "                                Read records from all files and insert them into database\n" +
            "load <file-name> <table-name>" +
            "           Reads records from <file-name> and inserts them into table <table-name>.\n" +
            "show <table-name>" +
            "                       Shows the contents of <table-name> row by row.\n" +
            "sql <some-sql-query>" +
            "                    Executes <some-sql-query> on the database\n" +
            "query <sql-number>" +
            "                      Executes a predefined sql query identified by <sql-number> (see query section).\n" +
            "report <report-number>" +
            "                  Prints the report identified by <report-number>.\n");


}

    private static class ExtractParameter {
        private String command;
        private String commandOp;
        private String commandPar;

        public ExtractParameter(String command) {
            this.command = command;
            commandOp=command;
            commandPar="";

        }

        public String getCommandOp() {
            return commandOp;
        }

        public String getCommandPar() {
            return commandPar;
        }

        public ExtractParameter invoke() {
            if (!(command.equals("help") || command.equals("create schema") ||
                    command.equals("drop schema") || command.equals("load all")) &&
                    command.indexOf(" ")>0){
                commandOp=command.substring(0, command.indexOf(" "));
                commandPar= command.substring(command.indexOf(" ")+1);
            }
            return this;
        }
    }
}