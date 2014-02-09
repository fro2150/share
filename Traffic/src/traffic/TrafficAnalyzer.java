package traffic;

import java.io.FileInputStream;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

/* New comment */

public class TrafficAnalyzer {
	final static String tag_key = "Key: ";
	final static String tag_read = "Bytes read: ";
	final static String tag_write = "Bytes written: ";
	final static String tag_calls = "Calls: ";

	static String fEncoding = "UTF-8";
	static SimpleDateFormat sdf = new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss" );

	StringBuilder input;
	Map <String, RequestSummary> requestMap = new HashMap <String, RequestSummary> ();
	List <Date> dates = new ArrayList <Date> ();
	List <RequestSummary> requestSummaries = new ArrayList <RequestSummary> ();
	Map <Date, List <Request>> dateMap = new HashMap <Date, List <Request>> ();
	Date lastDate;
	
	long totalReads = 0L;
	long totalWrites = 0L;
	long totalCalls = 0L;
	long interval = 0L;

	public static void main(String args[]) throws Exception {

		if (args.length < 1) {
			System.out.println ("Error : no file name specified");
			System.out.println ("Usage : logAnalyser [file path]");
			return;
		}
		TrafficAnalyzer ta = new TrafficAnalyzer ();
		ta.analyze (args[0]);
		ta.calculateStats();
		ta.summarize ();
	}

	void logError (int lineNumber, String error, String line) {
		System.out.println ("Parse error at line :" + lineNumber);
		System.out.println (error + "   " + line);
	}

	/** Read the contents of the given file. */
	void analyze (String fileName) throws IOException {
		input = new StringBuilder(); 
		//String NL = System.getProperty("line.separator");
		Scanner scanner = new Scanner(new FileInputStream(fileName), fEncoding);
		try {
			int lineNumber = 0;
			while (scanner.hasNextLine()){
				lineNumber++;
				String line = scanner.nextLine();
				if (line.startsWith("|")) {
					// Create a new date entry in the list and in the map
					try {
						addNewDate (sdf.parse (line.substring(2, 21)));
					} catch (ParseException e) {
						logError (lineNumber,  "expecting a date", line);
					}
				} else {	
					if (line.startsWith(tag_key)) {
						Request request = new Request ();
						request.name = line.substring(tag_key.length());
						if (!scanner.hasNextLine()) {
							logError (lineNumber,  "unexpected end of request", line);
						} else {
							line = scanner.nextLine();
							if (!line.startsWith(tag_read)) {
								logError (lineNumber,  "expected: " + tag_read, line);
							} else {
								request.reads = Long.parseLong(line.substring(tag_read.length()));
								if (!scanner.hasNextLine()) {
									logError (lineNumber,  "unexpected end of request", line);
								} else {
									line = scanner.nextLine();
									if (!line.startsWith(tag_write)) {
										logError (lineNumber,  "expected: " + tag_write, line);
									} else {
										request.writes = Long.parseLong(line.substring(tag_write.length()));
										if (!scanner.hasNextLine()) {
											logError (lineNumber,  "unexpected end of request", line);
										} else {
											line = scanner.nextLine();
											if (!line.startsWith(tag_calls)) {
												logError (lineNumber,  "expected: " + tag_calls, line);
											} else {
												request.calls = Long.parseLong(line.substring(tag_calls.length()));
												addRequest (request);
											}
										}
									}
								}
							}
						}
					}
				}
			}
		}
		finally{
			scanner.close();
		}
	}

	void summarize () {
		displayGlobalStats ();
		System.out.println (formatRSHeader());
		Collections.sort(requestSummaries, new Comparator<RequestSummary>() {
            public int compare(RequestSummary o1, RequestSummary o2) {
            	return new Double (o2.weight).compareTo( new Double (o1.weight));
            }
        });
		for (RequestSummary rs : requestSummaries) {
			System.out.println (rs.format());
		}
	}

	void calculateStats () {
		if (dates.size () >= 2) {
			interval = (dates.get(dates.size() -1).getTime() - dates.get(0).getTime())/1000;
			for (RequestSummary rs : requestSummaries) {
				totalReads += rs.reads;
				totalWrites += rs.writes;
				totalCalls += rs.calls;
				rs.readsPerSecond = ((double) rs.reads)/interval;
				rs.writesPerSecond = ((double) rs.writes)/interval;
				rs.callsPerSecond = ((double) rs.calls)/interval;
			}
			// calculate weight
			for (RequestSummary rs : requestSummaries) {
				rs.weight = 100*(double)(rs.reads + rs.writes)/(double)(totalReads+totalWrites);
			}
		}
	}

	void displayGlobalStats () {
		System.out.println ("Summary: ");
		System.out.println ("=========");
		System.out.println ("Start: " + dates.get(0).toString());
		System.out.println ("End: " + dates.get(dates.size()-1).toString());
		System.out.println ("Calls: " + totalCalls);
		System.out.println ("Bytes read: " + totalReads);
		System.out.println ( "Bytes written: " + totalWrites);
		System.out.println ("Calls/s: " + totalCalls/interval);
		System.out.println ("Bytes read/s: " + totalReads/interval);
		System.out.println ("Bytes written/s: " + totalWrites/interval);
		System.out.println ();
		System.out.println ();
	}

	void addNewDate (Date date) {
		dates.add(date);
		dateMap.put (date, new ArrayList <Request> ());
		lastDate = date;
	}

	void addRequest (Request request) {
		// Add the request to the date map
		dateMap.get(lastDate).add (request);

		// Compute the summary
		RequestSummary rs = getOrCreateRequestSummary (request);
		rs.addRequest (request);
	}

	static String formatRSHeader () {
		return String.format("%10s | %10s | %7s | %12s | %7s | %12s | %7s | %s",  "% Traffic",  " CALLS", "Ca/s", "READS", "Re/s", "WRITES", "Wr/s", "REQUEST");
	}

	RequestSummary getOrCreateRequestSummary (Request request) {
		RequestSummary rs = requestMap.get(request.name);
		if (rs == null) {
			rs = new RequestSummary ();
			rs.requestName = request.name;
			requestSummaries.add(rs);
			requestMap.put(request.name, rs);
		}
		return rs;
	}



	class RequestSummary  {
		String requestName;
		long reads = 0L;
		long writes = 0L;;
		long calls = 0L;
		double readsPerSecond = 0d;
		double writesPerSecond = 0d;
		double callsPerSecond = 0d;
		double weight= 0d;
		
		List <Request> requests = new ArrayList <Request> ();

		void addRequest (Request request) {
			reads += request.reads;
			writes += request.writes;
			calls += request.calls;
		}

		String format () {
			return String.format("%9.1f%% | %10d | %7.1f | %12d | %7.1f | %12d | %7.1f | %s",  
					weight, calls, callsPerSecond, reads, readsPerSecond, writes, writesPerSecond, requestName);
		}

	}

	class Request {
		String name;
		long reads = 0L;
		long writes = 0L;;
		long calls = 0L;
	}
}
