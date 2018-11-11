package mysqlcsv;


import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.QuoteMode;

import static mysqlcsv.ShCmd.*;

public class xdump {
	public static void main(String[] args) throws Exception {
		 Map<String, Integer> vParams = new HashMap();
		 vParams.put("u", 3);
	     vParams.put("p", 11);
	     vParams.put("o", 3);
	     vParams.put("append", 3);
	     vParams.put("h", 3);
	     vParams.put("P", 3);
	     vParams.put("f", 3);
	     vParams.put("t", 0);
	     vParams.put("q", 0);
	     vParams.put("no-header", 0);
	     vParams.put("use-compression", 3);
	     vParams.put("jdbc-driver", 3);
	     vParams.put("connection-string", 3);
	     vParams.put("output-encode", 3);
	     vParams.put("prop-useCompression", 3);
	     vParams.put("query", 3);
	     vParams.put("buffer", 3);
	     vParams.put("csv-quote", 3);
	     vParams.put("csv-escape", 3);
	     vParams.put("ignore-empty-lines", 3);
	     vParams.put("csv-delimiter", 3);
	     vParams.put("csv-quote-mode", 3);
	     vParams.put("i", 3);
	     Map<String, String> prs = parseArgs(args, false,vParams);
	     
	     LinkedHashMap<String, String> drivers=new java.util.LinkedHashMap();
	     if(prs.containsKey("help")) {
	    	 System.out.println("-t  ( DEFAULT|EXCEL|MYSQL|ORACLE|POSTGRESQL_CSV|POSTGRESQL_TEXT|RFC4180|TDF)");
	    	 System.out.println("--csv-quote-mode=(NONE|NON_NUMERIC|MINIMAL|ALL_NON_NULL|ALL)");
	    	 
	    	 for(String k:prs.keySet()) {
	    		 System.out.println(k+":");
	    	 }
	    	 System.exit(0);
	     }
	     
	     CSVFormat format = CSVFormat.DEFAULT;
    	 if("excel".equalsIgnoreCase(prs.get("t")))format = CSVFormat.EXCEL;
    	 if("mysql".equalsIgnoreCase(prs.get("t")))format =CSVFormat.MYSQL;
    	 if("oracle".equalsIgnoreCase(prs.get("t")))format =CSVFormat.ORACLE;
    	 if("postgresql".equalsIgnoreCase(prs.get("t")))format =CSVFormat.POSTGRESQL_CSV;
    	 if("postgretext".equalsIgnoreCase(prs.get("t")))format =CSVFormat.POSTGRESQL_TEXT;
    	 if("rfc4180".equalsIgnoreCase(prs.get("t")))format =CSVFormat.RFC4180;
    	 if("tdf".equalsIgnoreCase(prs.get("t")))format =CSVFormat.TDF;
    	 if("tsv".equalsIgnoreCase(prs.get("t")))format =format.withDelimiter('\t');
    	 
    	 if(prs.containsKey("csv-quote"))format.withQuote(prs.get("csv-quote").charAt(0));
    	 if(prs.containsKey("csv-escape"))format.withEscape(prs.get("csv-escape").charAt(0));
    	 if(prs.containsKey("ignore-empty-lines"))format.withIgnoreEmptyLines(!"false".equalsIgnoreCase(prs.get("ignore-empty-lines")));
    	 if(prs.containsKey("csv-delimiter"))format.withDelimiter(prs.get("csv-delimiter").charAt(0));
    	 if(prs.containsKey("csv-quote-mode")) {
    		 if("NONE".equalsIgnoreCase(prs.get("csv-quote-mode")))format.withQuoteMode(QuoteMode.NONE);
    		 if("NON_NUMERIC".equalsIgnoreCase(prs.get("csv-quote-mode")))format.withQuoteMode(QuoteMode.NON_NUMERIC);
    		 if("MINIMAL".equalsIgnoreCase(prs.get("csv-quote-mode")))format.withQuoteMode(QuoteMode.MINIMAL);
    		 if("ALL_NON_NULL".equalsIgnoreCase(prs.get("csv-quote-mode")))format.withQuoteMode(QuoteMode.ALL_NON_NULL);
    		 if("ALL".equalsIgnoreCase(prs.get("csv-quote-mode")))format.withQuoteMode(QuoteMode.ALL);
    	 }
	     drivers.put("com.mysql.cj.jdbc.Driver", "jdbc:mysql://" + 
	    	     ((prs.containsKey("h")?prs.get("h"):"localhost"))   +
	    	     (prs.containsKey("P")?(":"+prs.get("P"))+"/":"/") +
	    	     ((prs.containsKey("0")?prs.get("0"):"")
	    	    ));
	     drivers.put("oracle.jdbc.driver.OracleDriver","jdbc:oracle:thin:@"+((prs.containsKey("h")?prs.get("h"):"localhost"))+":"+(prs.containsKey("P")?prs.get("P"):"1521")+":"+ (prs.containsKey("0")?prs.get("0"):""));
	     drivers.put("org.postgresql.Driver","jdbc:postgresql://"+((prs.containsKey("h")?prs.get("h"):"localhost"))+":"+(prs.containsKey("P")?prs.get("P"):"5432")+"/"+ (prs.containsKey("0")?prs.get("0"):""));
	     drivers.put("org.sqlite.JDBC","jdbc:sqlite:" +(prs.containsKey("0")?prs.get("0"):""));
	     drivers.put("org.apache.cassandra.cql.jdbc.CassandraDriver","jdbc:cassandra:/root/root@"+((prs.containsKey("h")?prs.get("h"):"localhost"))+":"+(prs.containsKey("P")?prs.get("P"):"9160")+"/" +(prs.containsKey("0")?prs.get("0"):""));
	    String driver=prs.containsKey("jdbc-driver")?prs.get("jdbc-driver"):"com.mysql.cj.jdbc.Driver";
	   
	     if(prs.containsKey("jdbc-driver")) {
	    	 if(prs.get("jdbc-driver").matches("\\d+")) {
	    		driver=(String) new ArrayList(drivers.keySet()).get(Integer.parseInt(prs.get("jdbc-driver")));
	    	 }else {
	    		 for(String t:drivers.keySet()) {
	    			 if(t.toLowerCase().contains(prs.get("jdbc-driver").toLowerCase())) {
	    				 driver=t;
	    				 break;
	    			 }
	    		 };
	    	 }
	     }
	     String connectUrl=prs.containsKey("connection-string")?prs.get("connection-string"):drivers.get(driver);
	     
	     Class.forName(driver);
	     Properties ps = new Properties();
	   
	  ps.setProperty("useCompression",   String.valueOf("".equals(prs.get("use-compression")) || "1".equals(prs.get("use-compression")) || "on".equalsIgnoreCase(prs.get("use-compression")) || "true".equalsIgnoreCase(prs.get("use-compression"))));
	        if (prs.containsKey("u")) {
	            ps.put("user", prs.get("u"));
	        }
	        if (prs.containsKey("p")) {
	            ps.put("password", prs.get("p"));
	        }
	     
	        for(String k:prs.keySet()) {
	        	if(k.startsWith("prop-")) {
	        		ps.put(k.substring(5), prs.get(k));
	        	}
	        }
	     Connection conn = DriverManager.getConnection(connectUrl,ps);
	     conn.createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY, java.sql.ResultSet.CONCUR_READ_ONLY);
	     int buffer=Math.max(0,prs.containsKey("buffer")?Integer.parseInt(prs.get("buffer")):prs.containsKey("o")?1024*1024:1024*8);
	     
	    PreparedStatement stmt = conn.prepareStatement(prs.containsKey("query")?prs.get("query"):"select * from " +prs.get("1"));
	    stmt.setFetchSize(Integer.MIN_VALUE);
	    
	    ResultSet rs = stmt.executeQuery();
	    
	    ResultSetMetaData mt = rs.getMetaData();
	    int cnt=0;
	    long t = System.currentTimeMillis();
	    
	    int cl = mt.getColumnCount();
	    if(prs.containsKey("o") && !prs.containsKey("f")&& !prs.containsKey("append") && new File(prs.get("o")).exists()) {
	    	System.err.println("file "+prs.get("o")+" exists!");
	    	System.exit(1);
	    }
	    
	    OutputStream out=prs.containsKey("o")?
	    		new FileOutputStream(new File(prs.get("o"))):
	    			
	    						System.out;
	   
	    CSVPrinter csv =null;
	   
	    if(prs.containsKey("output-encode")) {
	    	 csv=new CSVPrinter(new BufferedWriter(new OutputStreamWriter(out,prs.get("output-encode")),buffer), format );
	    }else {
	    	 csv=new CSVPrinter(new BufferedWriter(new OutputStreamWriter(out),buffer), format );
	    }
	   
	    
	    if(!prs.containsKey("no-header")) {
	    	for(int c=1;c<=cl;c++) {
	    	  csv.print(mt.getColumnName(c));
	    	}
	    	csv.println();
	    }
	    csv.flush();
	    long t0=System.currentTimeMillis();
		long[] tis=new long[] {t0,t0,t0,t0,t0};
	    long[] ris=new long[] {0,0,0,0,0};
	    int c=0;
	    int c2=0;
	    boolean isQ=prs.containsKey("q");
	    while(rs.next()) {
	    	
	    	 for (int i = 1; i <= cl; i++) {
           	  csv.print(rs.getObject(i));
             }
             csv.println();
             if(isQ)continue;
             cnt++;
 	    	c2++;
	    	if(cnt%1000==0 && ((t0=System.currentTimeMillis())-t>3000 || cnt%100000==0)) {
	    		tis[c%5]=t0;
	    		ris[c%5]=c2;
	    		c2=0;
	    		t = t0;
	    		if(prs.containsKey("o")) {
	    			System.out.println(cnt+"\t\t"+ (int)((ris[0]+ris[1]+ris[2]+ris[3]+ris[4])/Math.max(0.001,((t0-tis[(c+1)%5])/1000.0)))+"/s");
	    		}
	    		else {
	    			System.err.println(cnt+"\t\t"+ (int)((ris[0]+ris[1]+ris[2]+ris[3]+ris[4])/Math.max(0.001,((t0-tis[(c+1)%5])/1000.0)))+"/s");
	    		}
	    		c++;
	    	}	
	    }
	     System.exit(cnt>0?0:1); 
	}
}

