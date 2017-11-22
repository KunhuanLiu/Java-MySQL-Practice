package com.ef;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Scanner;



public class MySQLAccess {
	private Connection connect = null;
	private Statement statement = null;
	private ResultSet resultSet = null;
	private PreparedStatement blockstmt = null;
	private String host = "localhost:3306/accesslogSample";
	private String user = "logger";
	private String passwd = "Kunhuan_liu$presented";


	public MySQLAccess() {
		Scanner scan = new Scanner(System.in);
		System.out.printf("Current database: [%s]. If you wish to change, enter the host address; otherwise, enter nothing.\n",this.host);
		String input = scan.nextLine();
		while (!input.isEmpty()) {
			this.host = input;
			System.out.printf("Current host: [%s]. If you wish to change, enter the host address; otherwise, enter nothing.\n",this.host);
			input = scan.nextLine();
			
		}
		System.out.printf("Current username: [%s]. If you wish to change, enter the name; otherwise, enter nothing.\n",this.user);
		input = scan.nextLine();
		while (!input.isEmpty()) {
			this.user = input;
			System.out.printf("Current username: [%s]. If you still wish to change, enter the name; otherwise, enter nothing.\n",this.user);
			input = scan.nextLine();
		}
		System.out.printf("Current password: [%s]. If you wish to change, enter the new password; otherwise, enter nothing.\n",this.passwd);
		input = scan.nextLine();
		while (!input.isEmpty()) {
			this.passwd = input;
			System.out.printf("Current password: [%s]. If you still wish to change, enter another password; otherwise, enter nothing.\n",this.passwd);
			input = scan.nextLine();
		}
		try {
			// This will load the MySQL driver, each DB has its own driver
			Class.forName("com.mysql.jdbc.Driver");

			// Setup the connection with the DB
			connect = DriverManager
					.getConnection("jdbc:mysql://" + this.host+"?useSSL=false",this.user,this.passwd);
			//as an interview test sample, I won't bother using certificate...
			System.out.println("Successfully Established connection to "+connect.getMetaData().getURL());
			this.statement = connect.createStatement();
			//deleting existing tables
			String sql = "DROP TABLE IF EXISTS `blocked_ip`;";
			this.statement.executeUpdate(sql);
			sql = "DROP TABLE IF EXISTS `log_of_request`;";
			this.statement.executeUpdate(sql);
			System.out.println("creating tables...");
			//creating two tables
			sql = "CREATE TABLE if not exists `log_of_request` (" + 
					"  `date` datetime(3) NOT NULL," + 
					"  `ip_address` VARCHAR(15) NOT NULL," + 
					"  `request` TINYTEXT NOT NULL," + 
					"  `status` INT NULL," + 
					"  `user agent` TINYTEXT NULL" + 
					");";
			this.statement.executeUpdate(sql);
			sql = "ALTER TABLE `log_of_request` ADD PRIMARY KEY(`date`, `ip_address`);";
			this.statement.executeUpdate(sql);
			sql = "CREATE INDEX idx_ip ON log_of_request (`ip_address`);";
			this.statement.executeUpdate(sql);
			sql = "CREATE TABLE if not exists `blocked_ip` (" + 
					"  `ip_address` VARCHAR(15) NOT NULL," + 
					"  `blocked_reason` TEXT(0) NOT NULL," + 
					"  PRIMARY KEY (`ip_address`)," + 
					"  CONSTRAINT `ip_address`" + 
					"    FOREIGN KEY (`ip_address`)" + 
					"    REFERENCES `log_of_request` (`ip_address`)" + 
					"    ON DELETE NO ACTION" + 
					"    ON UPDATE NO ACTION);";
			this.statement.executeUpdate(sql);
			System.out.println("Tables created...");
			
			
		}
		catch(SQLException se){
		      //Handle errors for JDBC
		      se.printStackTrace();
		      System.exit(1);
		}
		catch(Exception e) {
			//other errors, Class.forName...
			e.printStackTrace();
			System.exit(1);
		}
		scan.close();
	}
	
	public void insertLog(String date, String ip, String request, int status, String agent) throws SQLException {
		String sql = String.format("insert into  `log_of_request` values ('%s','%s',%s,%d,%s);", date,ip,request,status,agent);
		//System.out.println(sql);
		this.statement.executeUpdate(sql);
	}
	
	public ResultSet queryLog(String sql) throws SQLException {
		this.resultSet = statement.executeQuery(sql);
		return this.resultSet; //instance variable resultSet will be closed in this class
	}
	
	public void insertBlockedIp(String ip, String reason) throws SQLException {
		String sql = String.format("insert into  `blocked_ip` values ('%s','%s');", ip, reason);
		this.blockstmt = connect.prepareStatement(sql);
		blockstmt.executeUpdate();
	}
	
	public void close() {
		try {
			if (resultSet != null) {
				resultSet.close();
			}

			if (statement != null) {
				statement.close();
			}
			if (blockstmt != null) {
				blockstmt.close();
			}

			if (connect != null) {
				connect.close();
			}
		} catch (Exception e) {

		}
	}


}
