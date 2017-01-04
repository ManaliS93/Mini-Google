import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Scanner;
import java.util.UUID;
import java.util.Map.Entry;

public class Master {
	static String myIp, nsIp;
	static int myPort, nsPort;
	static String ipport[];
	static ServerSocket s;
	static String gotreq[];
	static Queue<HelperConfig> inactive_helpers = new LinkedList<HelperConfig>();
	static Queue<HelperConfig> active_helpers = new LinkedList<HelperConfig>();
	static Queue<String> c_index_q = new LinkedList<String>();
	static Queue<String> c_search_q = new LinkedList<String>();
	static Queue<String> resultIndex = new LinkedList<String>();
	static Queue<String> resultSearch = new LinkedList<String>();
	static Queue<String> NS_reply = new LinkedList<String>();
	static boolean helpFlag = true,flag=false;
	static HashMap<String, SQTracker> req_tracker = new HashMap<String, SQTracker>();
	 static HashMap<String,Integer> reasultGroup = new HashMap<String,Integer>();
	static Map<String, List<IndexStore>> map = new HashMap<String, List<IndexStore>>();
	static List<IndexStore> arl = new ArrayList<IndexStore>();
	static int builder_count=0;
	// static Queue<HelperConfig> active_helpers=new LinkedList<HelperConfig>();

	public static void init() throws IOException {

		InetAddress IP = InetAddress.getLocalHost();
		myIp = IP.getHostAddress();
		System.out.println("IP of my system is := " + myIp);
		s = new ServerSocket(0);
		myPort = s.getLocalPort();
		System.out.println("listening on port: " + myPort);
		BufferedReader br = new BufferedReader(new FileReader("ns.txt"));
		String line = br.readLine();
		ipport = line.split("//");
		nsIp = ipport[0];
		nsPort = Integer.parseInt(ipport[1]);
		System.out.println(nsIp + "" + nsPort);
		Thread ServerListenerThread = new Thread(ServerListener);
		ServerListenerThread.start();
		Socket socket = null;
		NS_send(Msg_Encoder("M_register"));
	}

	public static void NS_send(String msg) {
		Socket socket = null;
		try {
			socket = new Socket(nsIp, nsPort);

			DataOutputStream d = new DataOutputStream(socket.getOutputStream());

			d.writeUTF(msg);

			d.close();
			socket.close();

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public static String Msg_Encoder(String type) {
		String msg = "";
		switch (type) {
		case "M_register":
			msg = "M_register//" + myIp + "//" + myPort;
			break;
		case "M_help":
			msg = "M_help//";

		}
		return msg;

	}

	static Runnable ServerListener = new Runnable() {
		int temp_num = 0;
		HelperConfig h = null;

		@Override
		public void run() {
			Thread ServiceNSReplyThread = new Thread(ServiceNSReply);
			ServiceNSReplyThread.start();
			Thread ServiceIndexThread = new Thread(ServiceIndex);
			ServiceIndexThread.start();
			Thread IndexBuilderThread = new Thread(IndexBuilder);
			IndexBuilderThread.start();
			Thread ServiceSearchThread = new Thread(ServiceSearch);
			ServiceSearchThread.start();
			Thread ReasultSearchGroupThread = new Thread(ReasultSearchGroup);
			ReasultSearchGroupThread.start();
			System.out.println("Server Waiting for request");
			while (true) {
				String str;
				


				Socket socketL;
				try {
					socketL = s.accept();
					DataInputStream din = new DataInputStream(socketL.getInputStream());
					str = din.readUTF();
					//System.out.println("Rceived : "+str);
					//System.out.println(str.subSequence(0, 12).equals("resultSearch")+"-"+str.subSequence(0, 11));
					if (str.substring(0, 5).equals("index")&&( !str.contains("@"))) {
						synchronized (this) {
							//System.out.println("Added in c_index");
							c_index_q.add(str);
						}
					} else if (str.subSequence(0, 11).equals("resultIndex") && (!str.contains("#"))) {
						synchronized (this) {
							//System.out.println("Added in resultindex");
							resultIndex.add(str);
							builder_count--;
							if(builder_count==0)
							{
								//updater();
								flag=true;
								while(!active_helpers.isEmpty()){
									inactive_helpers.add(active_helpers.poll());
								}
								System.out.println("Done.");
							}
						
						}

					}
					 else if (str.subSequence(0, 12).equals("resultSearch") && str.contains("#")) {
							synchronized (this) {
								//System.out.println("Added result search");
								resultSearch.add(str);
							}

						}

					else {
						gotreq = str.split("//");

						switch (gotreq[0]) {
						case "NS_ack_help":
							synchronized (this) {
								NS_reply.add(str);
							}
							break;

						case "search":
							synchronized (this) {
								c_search_q.add(str);
							}
							break;
						case "HeartBeat//":
							System.out.println("Heatbeat");
							HelperSend("alive//",gotreq[1],Integer.parseInt(gotreq[2]));
							
						}
					}
				} catch (Exception e) {
					e.printStackTrace();
				}

			}
		}
	};

	public static synchronized void index_service() throws Exception {
		String temp[];
		String path;
		if (!c_index_q.isEmpty()) {
			System.out.println();
			temp = c_index_q.poll().split("#");
			System.out.println(temp);
			path = temp[1];

			if (inactive_helpers.size() > 0) {

				HelperConfig h;
				int num = inactive_helpers.size();
				//System.out.println("six=zeee" + num);
				String ip[] = new String[num];
				int port[] = new int[num];

				for (int j = 0; j < num; j++) {

					h = inactive_helpers.poll();
					h.isBusy = true;
					active_helpers.add(h);
					ip[j] = h.ip;
					port[j] = h.port;
					//System.out.println("haiii " + port[j] + " " + ip[j]);

				}
				displayIt(new File(path), ip, port, num);

			} else {
				if (helpFlag) {
					System.out.println("called $$");
					NS_send("M_help");
					Thread.sleep(2000);
					index_service();
				}

			}

		}
	}

	public static synchronized void updater() throws Exception, UnsupportedEncodingException {
		File dir = new File("/afs/cs.pitt.edu/usr0/mcs116/public/MIndex");
		String item;
		// attempt to create the directory here
		if (!dir.exists()) {
			boolean successful = dir.mkdir();
		}
		for (int i = 97; i < 123; i++) {
			PrintWriter pw = new PrintWriter(dir + "/" + (char) i + ".txt", "UTF-8");
			pw.write("");
			pw.close();
		}//System.out.println("here ++++");
		for (Entry<?, ?> e : map.entrySet()) {
			//System.out.println("Key: " + e.getKey() + " Value: ");
			List<IndexStore> var = (List<IndexStore>) e.getValue();
			for (int i = 0; i < var.size(); i++) {
				item = (String) e.getKey();
				//System.out.println(item.substring(0, 0));
				IndexStore tempIS = var.get(i);
				FileWriter fw = new FileWriter(dir + "/" + item.substring(0, 1) + ".txt", true);
				fw.append("Key#" + e.getKey());
				fw.append("#" + tempIS.path + "#" + tempIS.value + "\n");
				fw.append(System.lineSeparator());
				fw.close();

			}
		}
	System.out.println("Writing")
	}

	public static synchronized void displayIt(File node, String ip[], int port[], int num) throws Exception {
		String line = null, name, final_string = "";

		int i = 0, count = 0;
		String messeges[] = new String[num];

		Scanner in;
		System.out.println("----" + node.getAbsoluteFile());
		File f;
		if (node.getAbsoluteFile().isFile()) {
			f = new File(node.getPath());

			in = new Scanner(f);

			while (in.hasNext()) {
				i++;
				line = in.nextLine();
				final_string += " " + line.replaceAll("([^a-zA-Z']+)'*\\1*", " ");
				if (i % 50 == 0) {
					// System.out.println("count is : "+count+" "+ip[count]+"
					// "+port[count]);
					System.out.println("count is : " + count);
					// HelperSend("path#"+f,ip[count],port[count]);
					HelperSend("path#" + f + "#" + final_string + "#" + myIp + "#" + myPort, ip[count], port[count]);
					final_string = "";
					count++;
					builder_count++;
					System.out.println("count is" + count);
					if (count == num)
						count = 0;

					//System.out.println("mod 3 ho gaya idhar ***************");
				}

				//System.out.println(line);
				if (!in.hasNext() && !final_string.isEmpty()) {
					// HelperSend("path#"+f,ip[count],port[count]);
					HelperSend("path#" + f + "#" + final_string + "#" + myIp + "#" + myPort, ip[count], port[count]);
					final_string = "";
					count++;
					builder_count++;
					if (count == num)
						count = 0;

				}
			}
		}

		if (node.isDirectory()) {
			String[] subNote = node.list();
			for (String filename : subNote) {
				displayIt(new File(node, filename), ip, port, num);
			}
		}

	}

	public static synchronized void HelperSend(String msg, String ip, int port) {
		Socket socket = null;
		try {
			System.out.println("---------------" + ip + "-------" + port + " on Helpersend");
			socket = new Socket(ip, port);

			DataOutputStream d = new DataOutputStream(socket.getOutputStream());

			d.writeUTF(msg);

			d.close();
			socket.close();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static synchronized void search_service() {
		String temp[];
		String msg = "";
		SQTracker temp_counter;
		if (!c_search_q.isEmpty()) {
			String uid = UUID.randomUUID().toString();
			
			temp = c_search_q.poll().split("//");
			
			int u = 2, count = 0;
			if (inactive_helpers.size() > 0) {

				HelperConfig h;
				int num = inactive_helpers.size();
				System.out.println("sizeee" + num);
				String ip[] = new String[num];
				int port[] = new int[num];
				int temp_index=Integer.parseInt(temp[1])+3;
				System.out.println("======="+temp[temp_index]+"====="+temp_index);
				SQTracker sq=new SQTracker(temp[Integer.parseInt(temp[1])+3], Integer.parseInt(temp[Integer.parseInt(temp[1])+4]), "",0);
				req_tracker.put(uid, sq);
				for (int j = 0; j < num; j++) {

					h = inactive_helpers.poll();
					h.isBusy = true;
					active_helpers.add(h);
					ip[j] = h.ip;
					port[j] = h.port;
					System.out.println("haiii " + port[j] + " " + ip[j]);

				}
				for (int i = 1; i <= Integer.parseInt(temp[1])+1; i++) {
					msg += "//" + temp[u];
					u++;
					if (i % 3 == 0) {
						HelperSend("search"+"#"+msg+"#"+myIp+"#"+myPort+"#"+uid, ip[count], port[count]);
						count++;
						temp_counter=req_tracker.get(uid);
						temp_counter.counter+=1;
						req_tracker.put(uid, temp_counter);
						System.out.println("----------->>>"+req_tracker.get(uid).counter);
						if (count >= num)
							count = 0;
						msg = "";
					}
				}
				if(!msg.equals("")){
					HelperSend("search"+"#"+msg+"#"+myIp+"#"+myPort+"#"+uid, ip[count], port[count]);
					temp_counter=req_tracker.get(uid);
					temp_counter.counter+=1;
					System.out.println("----------->>>"+req_tracker.get(uid).counter);
					req_tracker.put(uid, temp_counter);
					msg="";
				}

			}
		}
	}

	public static synchronized void build_service() throws Exception, Exception {

		if (!resultIndex.isEmpty()) {
			String temp[];
			String path;
			HelperConfig h;
			IndexStore is;
			int number = 0;
			System.out.println();
			temp = resultIndex.poll().split("@");
			h = new HelperConfig(temp[1], Integer.parseInt(temp[2]), true);
			String item = temp[3];
			is = new IndexStore(temp[4], Integer.parseInt(temp[5]));

			if (map.containsKey(item)) {
				arl = map.get(item);
				boolean isPathPresent = false;
				for (int i = 0; i < arl.size(); i++) {
					String pathInObject = arl.get(i).path;
					if (pathInObject.equals(is.path)) {
						isPathPresent = true;
						number = i;
					}
				}
				if (isPathPresent) {
					//System.out.println("arl contains : " + is.path + " for " + item + "+");
					// System.out.println(arl.get(0).path+" for key "+item);
					arl.get(number).value = arl.get(number).value + is.value;
				} else {
					//System.out.println("arl DOES NOT contain : " + is.path + " for " + item);
					arl.add(is);
				}
				map.put(item, arl);
			} else {
				arl = new ArrayList<IndexStore>();
				//System.out.println("first time added " + is.path + " for " + item + "+");
				arl.add(is);
				map.put(item, arl);
			}

			for (Entry<?, ?> e : map.entrySet()) {
				//System.out.println("Key: " + e.getKey() + " Value: ");
				String key=(String) e.getKey();
				List<IndexStore> var = (List<IndexStore>) e.getValue();
				// var.sort(null);
				for(int k=0;k<var.size();k++)
				for (int i = 0; i < var.size()-1-k; i++) {
					IndexStore tempIS = var.get(i);
					//if (i + 1 < var.size()) {
						IndexStore temp2IS = var.get(i + 1);
						if (tempIS.value < temp2IS.value) {
							var.set(i, temp2IS);
							var.set(i + 1, tempIS);
						}
					//} else
						
					// System.out.println("Path: " + tempIS.path + " - Value: "
					// + tempIS.value);
					// String
					// msg="resultIndex"+"@"+myIp+"@"+myPort+"@"+e.getKey()+"@"+tempIS.path+"@"+tempIS.value;
				}
				map.put(key, var);
			}
			if(flag)
			updater();
			System.out.println("------------CURRENT INV INDEX------------");
			/*for (Entry<?, ?> e : map.entrySet()) {
				System.out.println("Key: " + e.getKey() + " Value: ");
				List<IndexStore> var = (List<IndexStore>) e.getValue();
				for (int i = 0; i < var.size(); i++) {
					IndexStore tempIS = var.get(i);
					System.out.println("Path: " + tempIS.path + " - Value: " + tempIS.value);
					// String
					// msg="resultIndex"+"@"+myIp+"@"+myPort+"@"+e.getKey()+"@"+tempIS.path+"@"+tempIS.value;

				}
			}*/

		}

	}

	public static synchronized void ReasultSearchGroup_service() {
		String temp[],result[] = null,a_r[]=null;
		String msg="",kv="";
		//ArrayList <String,Integer> forSort= new ArrayList<String,Integer>();
		ArrayList <IndexStore> forSort=new ArrayList<IndexStore>();

		if (!resultSearch.isEmpty()) {
			temp=resultSearch.poll().split("@");
			result=temp[2].split("#");
			/*for(int i=0;i<temp.length;i++)
			System.out.println("outer @ "+temp[i]);
			for(int i=0;i<result.length;i++)
				System.out.println("inner # "+result[i]);*/
			SQTracker sq=null;
			sq=req_tracker.get(temp[1]);
			//System.out.println("result 2 is   "+result[2]);
			for(int k=0;k<result.length;k++)
			{	if(result[k].equals(""))
				continue;
				
				a_r=result[k].split("!!");
				
				System.out.println("inside inside ----------------"+a_r[0]+"---- " +a_r[1]+"---"+a_r[2]);
				//sq.ans+=" "+a_r[0]+" "+a_r[1]+" "+a_r[2]+"\n";
				if(reasultGroup.containsKey(a_r[1]))
				{
					System.out.println("Adding xxx: "+a_r[1]);
					int freq=reasultGroup.get(a_r[1])+1;
					reasultGroup.put(a_r[1], freq);
				}
				else 
					reasultGroup.put(a_r[1],1);
			}
			//System.out.println("---------->>>>>"+req_tracker.get(temp[1]).counter);
			sq.counter-=1;
			
			req_tracker.put(temp[1], sq);
			//System.out.println("---------->>>>>"+req_tracker.get(temp[1]).counter);
			if (req_tracker.get(temp[1]).counter<=0){
				
				System.out.println("got all replies");
				System.out.println("reply is : "+req_tracker.get(temp[1]).ans);
				
				
				for (String key : reasultGroup.keySet()) {
					   System.out.println("------------------------------------------------");
					  
					   //System.out.println("key: " + key + " value: " + reasultGroup.get(key));
					   IndexStore IS=new IndexStore(key,reasultGroup.get(key));
					   forSort.add(IS);
					  
					}
				IndexStore temp_sort1,temp_sort2;
				
				for(int i = 0 ; i<forSort.size();i++)
					for (int j = 0; j < forSort.size()-i-1; j++)
				 {
					temp_sort1=forSort.get(j);
					//if (j+1<forSort.size())
					////{	
						temp_sort2=forSort.get(j+1);
						
						if(forSort.get(j).value < forSort.get(j+1).value)
						{
						
						forSort.set(j, temp_sort2);
						forSort.set(j+1, temp_sort1);
						}
					//}
					}
				for(int i = 0 ; i<forSort.size();i++)
					kv+=forSort.get(i).path+" contains "+forSort.get(i).value+" Keywords \n";
				msg="Result"+"#"+kv;
				HelperSend(msg,req_tracker.get(temp[1]).ip , req_tracker.get(temp[1]).port);
				while(!active_helpers.isEmpty()){
					inactive_helpers.add(active_helpers.poll());
				}
				reasultGroup.clear();
					
			}
			
			
		}
		


			
	}
	public static synchronized void NS_service() {

		if (!NS_reply.isEmpty()) {
			System.out.println("Inside method ");

			int temp_num = 0;
			HelperConfig h = null;
			gotreq = NS_reply.poll().split("//");
			temp_num = Integer.parseInt(gotreq[1]);
			int k = 2;
			for (int i = 0; i < temp_num; i++) {
				System.out.println(k + "---" + gotreq[k] + "---" + gotreq[k + 1]);
				h = new HelperConfig(gotreq[k], Integer.parseInt(gotreq[k + 1]), false);
				inactive_helpers.add(h);
				for (HelperConfig item : inactive_helpers) {
					System.out.println(item.port);
				}

				k = k + 2;
			}
		}
	}

	static Runnable ServiceNSReply = new Runnable() {
		public void run() {
			System.out.println("Service Ns started");
			while (true) {
				NS_service();
			}
		}
	};
	static Runnable ServiceIndex = new Runnable() {
		public void run() {
			System.out.println("Service Index started");
			while (true) {
				try {
					index_service();
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	};
	
	static Runnable IndexBuilder = new Runnable() {
		public void run() {
			System.out.println("IndexBuilder started");
			while (true) {
				try {
					build_service();
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	};
	static Runnable ReasultSearchGroup = new Runnable() {
		public void run() {
			System.out.println("ReasultSearchGroup started");
			while (true) {
				try {
					ReasultSearchGroup_service();
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	};
	static Runnable ServiceSearch = new Runnable() {
		public void run() {
			System.out.println("Service Searchs started");
			while (true) {
				search_service();
			}
		}
	};

	public static void main(String arg[]) throws InterruptedException {
		try {
			init();
			Thread.sleep(700);
			//System.out.println("here $$");
			NS_send("M_help");
			//Thread.sleep(70000);
			//updater();
			System.out.println("written");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
