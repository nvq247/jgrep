package mysqlcsv;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;
public class ShCmd {
	



		
		static int HAS_VALUE = 1;
		static int REQUIRE_VALUE = 2;
		static int MANDTORY = 4;
		static int INLINE_VALUE = 8;

		public static void main(String[] args1) throws UnsupportedEncodingException, Exception {
			
			 Map<String, Integer> vParams = new HashMap();
			 vParams.put("u", 3);
		     vParams.put("p", 11);
		     vParams.put("B", 3);
	        System.out.println(parseArgs(new String[]{"-u","root","-pxxx"}, false,vParams));
	   }


		private static String size(double i, String m) {
			String[] ms = new String[]{" Byte", " KB", " MB", " GB", " TB"};

			int x;
			for (x = 0; i / 1000.0D >= 1.0D; ++x) {
				i /= 1024.0D;
			}
			return (double) ((int) (i * 100.0D)) / 100.0D + ms[x] + m;
		}

		public static Map<String, String> parseArgs(String[] args, boolean acceptDup, Map<String, Integer> vParams) throws Exception {
			HashMap<String, String> rs = new HashMap();
			int ac = 0;
			String name = null;
			String[] var8 = args;
			int var7 = args.length;

			for (int var6 = 0; var6 < var7; ++var6) {
				String s = var8[var6];
				int var11;
				char[] var13;
				char c;
				int var17;
				 if (s.startsWith("--")) {
						int i = s.indexOf("=");
						if (i > 0) {
							String vv = s.substring(1 + i);
							if((vv.startsWith("'") && vv.endsWith("'")) || (vv.startsWith("\"") && vv.endsWith("\""))) {
								vv=vv.substring(1, vv.length()-1);
							}
							rs.put(s.substring(0, i).substring(2),vv );
						} else {
							rs.put(s.substring(2), "");
						}

						if (name != null) {
							var17 = (var13 = name.toCharArray()).length;

							for (var11 = 0; var11 < var17; ++var11) {
								c = var13[var11];
								rs.put(String.valueOf(c), "");
							}
						}

						name = null;
					} else
				if (s.startsWith("-")) {
					if (name != null) {
						rs.put(name, "");
					}

					name = null;
					String name1 = s.substring(1);
					var17 = (var13 = name1.toCharArray()).length;

					for (var11 = 0; var11 < var17; ++var11) {
						c = var13[var11];
						rs.put(String.valueOf(c), "");
						if (vParams.get("" + c) != null && (Integer.parseInt(((Integer) vParams.get("" + c)).toString()) & HAS_VALUE) > 0) {
							
							if(var11<name1.length()-1) {
								rs.put(String.valueOf(c), name1.substring(var11+1));
								name=null;
							}
							else if (vParams.get("" + c) != null && (Integer.parseInt(((Integer) vParams.get("" + c)).toString()) & INLINE_VALUE) > 0) {
								name=null;
							}
							else {
								name = String.valueOf(c);
							}
							
							break;
						}
					}
				} else {
					if (name != null) {
						rs.put(name, s);
						char[] var12;
						var11 = (var12 = name.toCharArray()).length;

						for (int var10 = 0; var10 < var11; ++var10) {
							char x= var12[var10];
							rs.put(String.valueOf(x), vParams.get("" + x) != null
									&& (Integer.parseInt(((Integer) vParams.get("" + x)).toString()) & HAS_VALUE) > 0
											? s
											: "");
						}
					} else if (!acceptDup && rs.values().contains(s)) {
						System.err.println("[31mexists 2 parameter's file:" + s + "!!!");
						System.exit(1);
					} else {
						rs.put(String.valueOf(ac++), s);
					}

					name = null;
				}
			}

			return rs;
		}
	}

