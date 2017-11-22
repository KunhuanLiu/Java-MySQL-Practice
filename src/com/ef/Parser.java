package com.ef;

import java.io.*;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
//import java.time.format.*;
//import java.time.temporal.ChronoField;
import java.util.Date;

public class Parser {
	Date startDate; //undefined
	Date endDate;
	int threshold;
	File logFile;
	MySQLAccess access;

	public Parser(String pathToFile, Date startDate, String duration, int threshold) {
		this.startDate = startDate;
		this.threshold = threshold;
		if (duration.equals("hourly")) {
			Calendar cal = Calendar.getInstance();
	        cal.setTime(startDate);
	        cal.add(Calendar.HOUR, 1);
			this.endDate = cal.getTime();
		}else { //duration == daily
			Calendar cal = Calendar.getInstance();
	        cal.setTime(startDate);
	        cal.add(Calendar.DATE, 1);
			this.endDate = cal.getTime();
		}
				
		logFile = new File(pathToFile);
		if (!logFile.exists()) {
			System.err.println("File access error: "+pathToFile+" is not a file or the file does not exist.\n"
					+ "If it is access.log, please specify the name; for example, \"./access.log\"\n");
			System.exit(1);
		}
		access = new MySQLAccess();
		
	}
	
	public void run() {
		this.load();
		this.blockIPs();
		access.close();
	}
	
	
	private void load() {
		BufferedReader reader;
		try {
			reader = new BufferedReader(new InputStreamReader(new FileInputStream(logFile)));
			String nextLine;
			int i = 0;
			System.out.println("Started parsing and updating tables...Please wait...");
			while ((nextLine = reader.readLine())!=null) {
				i++;
				if (i>=1000) {
					//do something to let user know you are working
					System.out.println("still working...");
					i = i%1000;
				}
				String[] arr= nextLine.split("\\|");
				
				if (arr.length==5) {
					//convert information
					//mysql can take string in the correct format
					String date = arr[0];
					String ip_address = arr[1];
					String request = arr[2];
					int status=-999;
					try {status = Integer.parseInt(arr[3]);
					}catch (NumberFormatException e) {
						System.out.println("number format error - status is not in number format: \n"+nextLine);
					}
					String agent = arr[4];
					try {
						access.insertLog(date, ip_address, request, status, agent);
					} catch (SQLException e) {
						System.out.println("SQLException encountered; detailed information: \n"+e.getMessage());
					}
				}else {
					System.out.printf("error - unrecognized log row: \n%s\n",nextLine);
				}
			}
		}catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//String NextLine=new String(reader.readLine().getBytes("UTF-8"), "UTF-8");

	}
	
	private void blockIPs() {
		SimpleDateFormat dateformat = new SimpleDateFormat("yyyy-MM-dd.HH:mm:ss.000");
		String sql = "SELECT ip_address, count(ip_address) " + 
				"FROM " + 
				"(SELECT * " + 
				"FROM `log_of_request` " + 
				"WHERE date Between '"+dateformat.format(startDate) +"' and '"+dateformat.format(endDate)+"' " + 
				") as log_in_range " + 
				"group by ip_address " + 
				"having count(ip_address)>="+this.threshold+";";
		System.out.println(sql);
		ResultSet results;
		try {
			results = access.queryLog(sql);
			System.out.printf("IPs that made more than %d requests during the duration: \n",this.threshold);
			while (results.next()) {
				int count = results.getInt(2);
				String ip = results.getString(1);
				System.out.printf("ip - %s : %d requests \n",ip, count);
				try {
				access.insertBlockedIp(ip, "requested "+count+" times between "+dateformat.format(startDate)+" - "+dateformat.format(endDate));
				}catch(SQLException e) {
					System.out.println("error - exception during inserting records into blocked_ip table.");
				}
			}
			System.out.println("IPs are added to table `blocked_ip`.");
			
		} catch (SQLException e) {
			System.out.println("Error - SQL query for obtaining select IPs failed.");
			e.printStackTrace();
		}
		
	}

	public static void main(String[] args) {
		Date startDate = null; //undefined
		String duration = null;
		int threshold = 0;
		String pathToFile = "access.log";
		SimpleDateFormat inputformat = new SimpleDateFormat("yyyy-MM-dd.HH:mm:ss");
		for (String s:args){
			//System.out.println(s);
			if (s.startsWith("--")) {
				//System.out.println(s.substring(2));
				String[] s_par = s.substring(2).toLowerCase().split("=");
				if (s_par.length==2) {
					switch (s_par[0]) {
					case "accesslog":
						pathToFile = s_par[1];
						break;
					case "startdate":
						try {
							startDate = inputformat.parse(s_par[1]);
						} catch (ParseException e) {
							System.err.println("ParseException : startDate cannot be parsed - "+s_par[1]);
							System.exit(1);
						}
						break;
					case "duration":
						if (s_par[1].equals("hourly")||s_par[1].equals("daily")) {
							duration = s_par[1];
						}
						else {
							System.err.println("duration is not recognized - "+s_par[1]);
							System.exit(1);
						}
							
						break;
					case "threshold":
						try {
							threshold = Integer.parseInt(s_par[1]);
						}catch (NumberFormatException nfe) {
							System.err.println("NumberFormatException : threshold needs to be an integer - "+s_par[1]);
							System.exit(1);
						}
						break;
					default:
						System.err.println("unrecognized variable name: "+s_par[0]);
						System.exit(1);
					}
				}
			}
		}
		if (pathToFile.equals("access.log")) {
			System.out.println("variable accesslog is not provided; it is assumed that the log file is called \"access.log\""
					+ " and is located in the working directory.");
		}
		//System.out.println(duration);
		Parser parser = new Parser(pathToFile, startDate, duration, threshold);
		parser.run();
		//System.out.println(startDate.getSeconds());
	}

}
