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
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.QuoteMode;

import static mysqlcsv.ShCmd.*;

public class xdump {
	private static final byte[] BOUDER = "####################".getBytes();

	public static void main(String[] args) throws Exception {
		 Map<String, Integer> vParams = new HashMap();
		 vParams.put("u#username", 3);
	     vParams.put("p#password", 11);
	     vParams.put("o#outputfile", 3);
	     vParams.put("append#", 3);
	     vParams.put("h#host", 3);
	     vParams.put("P#port", 3);
	     vParams.put("f#force", 3);
	     vParams.put("t#csv_type|DEFAULT|EXCEL|MYSQL|ORACLE|POSTGRESQL_CSV|POSTGRESQL_TEXT|RFC4180|TDF", 0);
	     vParams.put("q#quite", 0);
	     vParams.put("z#gzip", 0);
	     vParams.put("Z#zip", 0);
	     vParams.put("no-header#", 0);
	     vParams.put("use-compression#", 3);
	     vParams.put("jdbc-driver#driver_class|mysql|postgre|oracle|sqlite|cassandra", 3);
	     vParams.put("connection-string#connection string", 3);
	     vParams.put("output-encode#encode - utf-8|utf-16|..", 3);
	     vParams.put("prop-useCompression#1", 3);
	     vParams.put("query#select * from x", 3);
	     vParams.put("buffer#10240", 3);
	     vParams.put("csv-quote#\"", 3);
	     vParams.put("csv-escape#\\", 3);
	     vParams.put("ignore-empty-lines# ", 3);
	     vParams.put("csv-delimiter#,", 3);
	     vParams.put("csv-quote-mode#NONE|NON_NUMERIC|MINIMAL|ALL_NON_NULL|ALL", 3);
	     vParams.put("i#ignore  error", 3);
	    
	     LinkedHashMap<String, String> drivers=new java.util.LinkedHashMap();
	     Map<String, String> prs = parseArgs(new String[]{"-hnode1","-u","root","-Z","-p1234569@aA","tygia","-domain","-f","-o","/Users/nvq/xdump/","--use-compression","auction","alexa","coupon"}, false,vParams);
	     if(prs.containsKey("help") || !prs.containsKey("0") || prs.isEmpty()) {
	    	 for(String k:vParams.keySet()) {
	    		 String[] ts=(k+"# ").split("#");
	    		
	    		 System.out.println((ts[0].length()>1?"--":"-")+ts[0]+(ts[0].length()>1?"='"+ts[1]+"'":" : "+ts[1]));
	    	 }
	    	
	    	 System.exit(0);
	     }
	     {
		     Map<String, Integer> x = vParams;
		     vParams=new HashMap();
		     for(String k:x.keySet()) {
	    		 String[] ts=(k+"# ").split("#");
	    		 vParams.put(ts[0], x.get(k));
	    	 }
	     }
	     
	    prs = parseArgs(new String[]{"-hnode1","-u","root","-Z","-p1234569@aA","tygia","-domain","-f","-o","/Users/nvq/xdump/alldb.zip","--use-compression"}, false,vParams);
	     //Map<String, String> prs = parseArgs(args, false,vParams);
	     
	     CSVFormat format = CSVFormat.DEFAULT;
    	 if("excel".equalsIgnoreCase(prs.get("t")))format = CSVFormat.EXCEL;
    	 if("mysql".equalsIgnoreCase(prs.get("t")))format =CSVFormat.MYSQL;
    	 if("oracle".equalsIgnoreCase(prs.get("t")))format =CSVFormat.ORACLE;
    	 if("postgresql".equalsIgnoreCase(prs.get("t")))format =CSVFormat.POSTGRESQL_CSV;
    	 if("postgretext".equalsIgnoreCase(prs.get("t")))format =CSVFormat.POSTGRESQL_TEXT;
    	 if("rfc4180".equalsIgnoreCase(prs.get("t")))format =CSVFormat.RFC4180;
    	 if("tdf".equalsIgnoreCase(prs.get("t")))format =CSVFormat.TDF;
    	 if("tsv".equalsIgnoreCase(prs.get("t")))format =format.withDelimiter('\t');
    	 
    	 if(prs.containsKey("csv-quote"))format =format.withQuote(prs.get("csv-quote").charAt(0));
    	 if(prs.containsKey("csv-escape"))format =format.withEscape(prs.get("csv-escape").charAt(0));
    	 if(prs.containsKey("ignore-empty-lines"))format =format.withIgnoreEmptyLines(!"false".equalsIgnoreCase(prs.get("ignore-empty-lines")));
    	 if(prs.containsKey("csv-delimiter"))format =format.withDelimiter(prs.get("csv-delimiter").charAt(0));
    	 if(prs.containsKey("csv-quote-mode")) {
    		 if("NONE".equalsIgnoreCase(prs.get("csv-quote-mode")))format =format.withQuoteMode(QuoteMode.NONE);
    		 if("NON_NUMERIC".equalsIgnoreCase(prs.get("csv-quote-mode")))format =format.withQuoteMode(QuoteMode.NON_NUMERIC);
    		 if("MINIMAL".equalsIgnoreCase(prs.get("csv-quote-mode")))format =format.withQuoteMode(QuoteMode.MINIMAL);
    		 if("ALL_NON_NULL".equalsIgnoreCase(prs.get("csv-quote-mode")))format =format.withQuoteMode(QuoteMode.ALL_NON_NULL);
    		 if("ALL".equalsIgnoreCase(prs.get("csv-quote-mode")))format =format.withQuoteMode(QuoteMode.ALL);
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
	   
	  ps.setProperty("useCompression",   String.valueOf( (!prs.containsKey("use-compression") && driver.toLowerCase().contains("mysql")) || "".equals(prs.get("use-compression")) || "1".equals(prs.get("use-compression")) || "on".equalsIgnoreCase(prs.get("use-compression")) || "true".equalsIgnoreCase(prs.get("use-compression"))));
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
	     
	             //TABLE_CAT String => table catalog (may be null)
	    		 //TABLE_SCHEM String => table schema (may be null)
	    		 //TABLE_NAME String => table name
	    		 
	     if(!prs.containsKey("query") && !prs.containsKey("1") ) {
			ResultSet tables = conn.getMetaData().getTables(null, prs.get("0"), null, new String[] {"TABLE"});
			int t=1;
			while(tables.next()) {
				Object o1 = tables.getObject(1);
				if(prs.get("0").equalsIgnoreCase(o1.toString())) {
					String o2 = tables.getString("TABLE_NAME");
					prs.put(""+(t++), o2);
				}
			}
			tables.close();
	     }
	     
	     
	  
	    
	    int cnt=0;
	    long t = System.currentTimeMillis();
	    File o = prs.containsKey("o")?new File(prs.get("o")):null;
	   
	    if(o!=null && !prs.containsKey("f")&& !prs.containsKey("append") && (o.exists() && o.isFile())) {
	    	log("file "+prs.get("o")+" exists!",prs);
	    	System.exit(1);
	    }
	    
	    long t0=System.currentTimeMillis();
		long[] tis=new long[] {t0,t0,t0,t0,t0};
	    long[] ris=new long[] {0,0,0,0,0};
	    int c=0;
	    int c2=0;
	    ZipOutputStream zout =null;
	    GZIPOutputStream gzout =null;
	    OutputStream out =null;
	    boolean append=prs.containsKey("append");
	    for(int v=0;v<10000;v++) {
	    	String table=prs.get(""+v);
	    	
	    	
	    	if(table==null )break;
	    	
	    	if(!prs.containsKey("query") && v==0)continue;
	    	if(prs.containsKey("query"))table="query";
	    	
    			log("===================="+table+"========================",prs);
	    	
	    	if(o!=null ) {
	    		if(!o.exists()) {
	    			out=new FileOutputStream(o);
	    			if(prs.containsKey("Z")) {
	    				out=zout=new ZipOutputStream(out);
	    				zout.putNextEntry(new ZipEntry(table+".csv"));
		    		}
	    			else if(prs.containsKey("z")) {
    				    out=new FileOutputStream(o);
	    				out=gzout=new GZIPOutputStream(out);
		    		} else if(prs.containsKey("2") ) {
		    			if(!prs.containsKey("B")) {
    				    	out.write(BOUDER);
    				    	out.write((table+"\n").getBytes());
    				    }
		    		}
	    		}
	    		else if(o.exists() && o.isDirectory()) {
	    			
	    			if(prs.containsKey("Z")) {
	    				File f = new File(o+"/"+table+".csv.zip");
	    				if(f.exists() && !prs.containsKey("f")  ) {
	    					log("File "+f+" exists!!",prs);
	    					if(prs.containsKey("i"))continue;
	    					System.exit(1);
	    				}
    				    out=new FileOutputStream(f);
	    				out=zout=new ZipOutputStream(out);
	    				zout.putNextEntry(new ZipEntry(table+".csv"));
	    			}else if(prs.containsKey("z")){
	    				File f = new File(o+"/"+table+".csv.gz");
	    				if(f.exists() && !prs.containsKey("f") ) {
	    					log("File "+f+" exists!!",prs);
	    					if(prs.containsKey("i"))continue;
	    					System.exit(1);
	    				}
    				    out=new FileOutputStream(f);
	    				out=gzout=new GZIPOutputStream(out);
	    			}else {
	    				File f = new File(o+"/"+table+".csv");
	    				if(f.exists() && !prs.containsKey("f") && !append) {
	    					log("File "+f+" exists!!",prs);
	    					if(prs.containsKey("i"))continue;
	    					System.exit(1);
	    				}
	    				boolean exists = f.exists();
    				    out=new FileOutputStream(f,append);
    				    if(exists &&  append && !prs.containsKey("B")) {
    				    	out.write(BOUDER);
    				    	out.write((table+"\n").getBytes());
    				    }
	    			}
	    		}
	    		else if(o.exists() && o.isFile()) {
	    			if(out==null) {
	    				if(append && !prs.containsKey("z") && !prs.containsKey("Z")) {
	    					out=new FileOutputStream(o,append);
	    					if(!prs.containsKey("B")) {
	    				    	out.write(BOUDER);
	    				    	out.write((table+"\n").getBytes());
	    				    }
	    				}
	    				else if(prs.containsKey("f")) {
	    					out=new FileOutputStream(o);
	    					if(  prs.containsKey("Z")) {
	    						out=zout=new ZipOutputStream(out);
	    						zout.putNextEntry(new ZipEntry(table+".csv"));
	    					}else if(  prs.containsKey("z") ) {
		    					out=gzout=new GZIPOutputStream(out);
		    				}else if(prs.containsKey("2") ) {
				    			if(!prs.containsKey("B")) {
		    				    	out.write(BOUDER);
		    				    	out.write((table+"\n").getBytes());
		    				    }
				    		}
	    				}else {
	    					if( (prs.containsKey("z") || prs.containsKey("Z"))) {
		    					log("can not append an compress file: "+o,prs);
		    					
		    					System.exit(1);
		    				}else {
		    					log("file exists: "+o,prs);
		    					
		    					System.exit(1);
		    				}
	    				}
	    			}else {
		    					if(  prs.containsKey("Z")) {
		    						out.flush();
		    						zout.closeEntry();
		    						zout.putNextEntry(new ZipEntry(table+".csv"));
		    					}else  {
		    						if( !prs.containsKey("B")) {
		        				    	out.write(BOUDER);
		        				    	out.write((table+"\n").getBytes());
		        				    }
			    				}
		    				}
	    				
	    			}
	    		
	    		
	    	}else {
	    		if(out==null) {
	    			out=System.out;
	    			if(prs.containsKey("Z")) {
	    				out=zout=new ZipOutputStream(out);
						zout.putNextEntry(new ZipEntry(table+".csv"));
		    		}
	    			else if(prs.containsKey("z")) {
	    				out=gzout=new GZIPOutputStream(out);
		    		}
	    		}else {
	    			if(prs.containsKey("Z")) {
	    				out.flush();
	    				zout.closeEntry();
	    				zout.putNextEntry(new ZipEntry(table+".csv"));
		    		}
	    			else  {
	    				if( !prs.containsKey("B")) {
    				    	out.write(BOUDER);
    				    	out.write((table+"\n").getBytes());
    				    }
		    		}
	    		}
	    		
	    		
	    	}

	    	
	    	
	    	  PreparedStatement stmt = conn.prepareStatement(prs.containsKey("query")?prs.get("query"):"select * from " +table);
	    	  prs.remove("query");
	  	      stmt.setFetchSize(Integer.MIN_VALUE);
	  	    
	  	    ResultSet rs = stmt.executeQuery();
	  	    
	  	    ResultSetMetaData mt = rs.getMetaData();
	  	    int cl = mt.getColumnCount();
	  	    

	   
	   
	   int ts=0;
	    CSVPrinter csv =null;
	   
	    if(prs.containsKey("output-encode")) {
	    	 csv=new CSVPrinter(new BufferedWriter(new OutputStreamWriter(out,prs.get("output-encode")),buffer), format );
	    }else {
	    	 csv=new CSVPrinter(new BufferedWriter(new OutputStreamWriter(out),buffer), format );
	    }
	   
	    
	    if(!prs.containsKey("no-header")) {
	    	for(int x=1;x<=cl;x++) {
	    	  csv.print(mt.getColumnName(x));
	    	}
	    	csv.println();
	    }
	    csv.flush();
	   
	    boolean isQ=prs.containsKey("q");
	    while(rs.next()) {
	    	
	    	 for (int i = 1; i <= cl; i++) {
           	  csv.print(rs.getObject(i));
             }
             csv.println();
             if(isQ)continue;
             cnt++;
 	    	c2++;
 	    	ts++;
	    	if(cnt%1000==0 && ((t0=System.currentTimeMillis())-t>3000 || cnt%100000==0)) {
	    		tis[c%5]=t0;
	    		ris[c%5]=c2;
	    		c2=0;
	    		t = t0;
	    		
	    			log(cnt+"\t\t"+ (int)((ris[0]+ris[1]+ris[2]+ris[3]+ris[4])/Math.max(0.001,((t0-tis[(c+1)%5])/1000.0)))+"/s",prs);
	    		
	    		c++;
	    	}	
	    }
		csv.flush();
	    if(prs.containsKey("Z")) {
	    	zout.closeEntry();
	    }
	     if(o!=null && o.isDirectory()) {
	    	csv.close();
	    }
	   
	    
	    
	    rs.close();
	    stmt.close();
	    log("finisded "+ts+" records !!", prs);
	    }
	    try {
	    if(zout!=null) {
	    	zout.close();
	    }
	    else if(gzout!=null) {
	    	gzout.close();
	    }
	    }catch (Exception e) {
			// TODO: handle exception
		}
	     System.exit(cnt>0?0:1); 
	}

	private static void log(String string,Map<String,String> ps) {
		if(ps.containsKey("o")) {
			System.out.println(string);
		}else {
			System.err.println(string);
		}
	}
}

