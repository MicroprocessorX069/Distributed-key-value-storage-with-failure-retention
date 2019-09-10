package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Formatter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.content.SharedPreferences;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;
import android.widget.TextView;

import static android.content.ContentValues.TAG;
import static android.content.Context.TELEPHONY_SERVICE;

public class SimpleDynamoProvider extends ContentProvider {

	public String tag="Provider ";
	public static final String PREFS_NAME = "MyPrefsFile";
	static final int SERVER_PORT = 10000;
	String srcPort="";
		
	Integer arr[]={0,1};
	
	String[] hashtable;
	int counter=0;

	int[] modes={0,1,2,3,4,5,6,7,8,9};
	Set<Integer> mapSet=new HashSet<Integer>(Arrays.asList(arr));
	int[] ports={11124,11112,11108,11116,11120};
	String insertAv="0";
	String reboot="0";
	boolean recovering = false;
	int count =0;
	String hashVal="";
	String Ssr1="";
	String Ssr2="";
	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {

		SharedPreferences prefs = getContext().getSharedPreferences(PREFS_NAME, 0);
		if (selection.equals("@")) {

			prefs.edit().clear().apply();
		} else if (selection.equals("*")) {
			for (int i = 0; i < ports.length; i++) {
				Socket socket = null;
				String remoteport = Integer.toString(ports[i]);

				try {
					socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt(remoteport));
				} catch (Exception e) {
					Log.w(tag,e.toString());
				}

				int mod= 6;
				String msgToSend = null;
				StringBuilder tempBuilder=new StringBuilder().append(Integer.toString(mod)+"$").append(selection);
				msgToSend=tempBuilder.toString();
				OutputStream outToServer = null;


				try {
					outToServer = socket.getOutputStream();
				} catch (Exception e) {
					Log.w(tag,e.toString());
				}

				DataOutputStream out = new DataOutputStream(outToServer);


				try {
					out.writeUTF(msgToSend);
				} catch (Exception e) {
					Log.w(tag,e.toString());
				}
				try {
					Thread.sleep(30);
				} catch (Exception e) {
					Log.w(tag,e.toString());
				}
				try {
					out.flush();
				} catch (Exception e) {
					Log.w(tag,e.toString()); //TODO: Combine Try catches
				}


			}
		} else {
			String key = selection; //TODO: make string equal to string function
			String hash = null;

			try {
				hash = genHash(key);
			} catch (NoSuchAlgorithmException e) {
				Log.w(tag,e.toString());
			}
			int portInt=hashToPort(hash);
			String port_r=Integer.toString(portInt);

			if (port_r.equals(srcPort)) {
				prefs.edit().remove(selection).apply();
				String succ[]=getsucc(port_r);
				for(int i=0;i<2;i++)
				{
					Socket socket = null;
					String remoteport = succ[i];

					try {
						socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(remoteport));
					} catch (Exception e) {
						Log.w(tag,e.toString());
					}


					int mod= 7;
					String msgToSend = null;
					StringBuilder tempBuilder=new StringBuilder().append(Integer.toString(mod)+"$").append(selection);
					msgToSend=tempBuilder.toString();
					OutputStream outToServer = null;


					try {
						outToServer = socket.getOutputStream();
					} catch (Exception e) {
						Log.w(tag,e.toString());
					}


					DataOutputStream out = new DataOutputStream(outToServer);

					try {
						out.writeUTF(msgToSend);
					} catch (Exception e) {
						Log.w(tag,e.toString());
					}
					try {
						Thread.sleep(30);
					} catch (Exception e) {
						Log.w(tag,e.toString());
					}
					try {
						out.flush();
					} catch (Exception e) {
						Log.w(tag,e.toString());
					}


				}


			} else {
				Socket socket = null;
				String remoteport = port_r;

				try {
					socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt(remoteport));
				} catch (Exception e) {
					Log.w(tag,e.toString());
				}


				int mod= 7;
				String msgToSend = null;

				StringBuilder tempBuilder=new StringBuilder().append(Integer.toString(mod)+"$").append(selection);
				msgToSend=tempBuilder.toString();
				OutputStream outToServer = null;


				try {
					outToServer = socket.getOutputStream();
				} catch (Exception e) {
					Log.w(tag,e.toString());
				}


				DataOutputStream out = new DataOutputStream(outToServer);

				try {
					out.writeUTF(msgToSend);
				} catch (Exception e) {
					Log.w(tag,e.toString());
				}
				try {
					Thread.sleep(30);
				} catch (Exception e) {
					Log.w(tag,e.toString());
				}
				try {
					out.flush();
				} catch (Exception e) {
					Log.w(tag,e.toString());
				}


				String succ[]=getsucc(port_r);
				int i=0;
				Iterator<Integer> mItr=mapSet.iterator();
				while(mItr.hasNext())
				{
					i=mItr.next();
					remoteport = succ[i];

					try {
						socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(remoteport));
					} catch (Exception e) {
						Log.w(tag,e.toString());
					}


					mod= 7;
					msgToSend = "";
					tempBuilder=new StringBuilder().append(Integer.toString(mod)+"$").append(selection);
					msgToSend=tempBuilder.toString();
					outToServer = null;


					if (socket != null) {
						try {
							outToServer = socket.getOutputStream();
						} catch (Exception e) {
							Log.w(tag,e.toString());
						}
					}


					out = new DataOutputStream(outToServer);

					try {
						out.writeUTF(msgToSend);
					} catch (Exception e) {
						Log.w(tag,e.toString());
					}
					try {
						Thread.sleep(30);
					} catch (Exception e) {
						Log.w(tag,e.toString());
					}
					try {
						out.flush();
					} catch (Exception e) {
						Log.w(tag,e.toString());
					}
					//i++;

				}
			}
		}

		return 0;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	//ipart
	@Override
	public Uri insert(Uri uri, ContentValues values) {


		if(reboot.equals("1"))

		{
			try {
				Thread.sleep(5000);
			} catch (Exception e) {
				Log.w(tag,e.toString());
			}
		}
	
		insertAv="1";

		String key = values.getAsString("key");

		String hash = null;
		try {
			hash = genHash(key);
		} catch (NoSuchAlgorithmException e) {
			Log.w(tag,e.toString());
		}
		int portInt=hashToPort(hash);
		String port_r=Integer.toString(portInt);
		if(port_r.equals(srcPort))
		{
			SharedPreferences prefs = getContext().getSharedPreferences(PREFS_NAME, 0);
			SharedPreferences.Editor editor = prefs.edit();

			editor.putString(values.getAsString("key"), values.getAsString("value"));
			editor.apply();
			// TODO Auto-generated method stub

			String[] succ = getsucc(srcPort);
			Socket socket = null;
			int i=0;
			Iterator<Integer> mItr=mapSet.iterator();
			//while(mItr.hasNext()){
			while (i<2){

				String remoteport = succ[i];

				try {
					socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt(remoteport));
				} catch (Exception e) {
					Log.w(tag,e.toString());
				}


				String select = values.getAsString("key") + "$" + values.getAsString("value");
				int mod= 31;
				String msgToSend = null;
				StringBuilder tempBuilder=new StringBuilder().append(Integer.toString(mod)+"$").append(select);
				msgToSend=tempBuilder.toString();

				OutputStream outToServer = null;


				try {
					outToServer = socket.getOutputStream();
				} catch (Exception e) {
					Log.w(tag,e.toString());
				}


				DataOutputStream out = new DataOutputStream(outToServer);

				try {
					out.writeUTF(msgToSend);
				} catch (Exception e) {
					Log.w(tag,e.toString());
				}
				try {
					Thread.sleep(30);
				} catch (Exception e) {
					Log.w(tag,e.toString());
				}
				try {
					out.flush();
				} catch (Exception e) {
					Log.w(tag,e.toString());
				}


			i++;
			}

			insertAv="0";

			try {
				Thread.sleep(30);
			} catch (Exception e) {
				Log.w(tag,e.toString());
			}

			return uri;
		}
		else
		{
			String msg = "2$" + values.getAsString("key") + "$" + values.getAsString("value")+"$"+port_r;
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, srcPort);
			try {
				Thread.sleep(50);
			} catch (Exception e) {
				Log.w(tag,e.toString());
			}



		}


		return null;
	}

	@Override
	public boolean onCreate() {
		Context context = this.getContext();
		TelephonyManager tel = (TelephonyManager) context.getSystemService(TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));

		srcPort=myPort;
		try {
			hashVal=genHash(Integer.toString(Integer.parseInt(srcPort)/2));
		} catch (NoSuchAlgorithmException e) {
			Log.w(tag,e.toString());
		}
		String succ[]=getsucc(myPort);
		Ssr1=succ[0];Ssr2=succ[1];
		String portsString[]=new String[ports.length];
		for (int z=0;z<ports.length;z++){
			portsString[z]=Integer.toString(ports[z]);
		}
		hashtable = gethashtable(portsString);
		SharedPreferences prefs = getContext().getSharedPreferences(PREFS_NAME, 0);
		Map<String,String> mymap = (Map<String, String>) prefs.getAll();
		if(mymap.size()!=0)
		{
			recovering=true;
			reboot="1";

			try {

				prefs.edit().clear().apply();
				String msg = "1$" +myPort;
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, srcPort);

			} catch (Exception e) {
				Log.w(tag,e.toString());
			}
			//prefs.edit().clear().apply();



		}


		try {

			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
		}
		catch (Exception e)
		{

			Log.e(TAG, "Can't create a ServerSocket");

		}


		return false;
	}


	//stpart
	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

		public Uri buildUri(String scheme, String authority) {
			Uri.Builder uriBuilder = new Uri.Builder();
			uriBuilder.authority(authority);
			uriBuilder.scheme(scheme);
			return uriBuilder.build();
		}






		ContentValues keyValueToInsert = new ContentValues();

		@Override
		protected Void doInBackground(ServerSocket... sockets) {

			ServerSocket serverSocket = sockets[0];

			while (true) {


				Socket server = null;
				try {
					server = serverSocket.accept();
				} catch (Exception e) {
					Log.w(tag,e.toString());
				}

				DataInputStream in = null;
				try {
					in = new DataInputStream(server.getInputStream());
				} catch (Exception e) {
					Log.w(tag,e.toString());
				}
				String msg2 = null;
				try {
					msg2 = in.readUTF();
				} catch (Exception e) {
					Log.w(tag,e.toString());
				}
				String[] parts = msg2.split("\\$");
				int mode = Integer.parseInt(parts[0]);



				switch (mode) {


					//if(mode==3)
					case 7: {
						String selection = parts[1];
						SharedPreferences prefs = getContext().getSharedPreferences(PREFS_NAME, 0);
						prefs.edit().remove(selection).apply();
						break;
					}
					//else if (mode==31)
					case 31: {
						String value = parts[2];
						String key = parts[1];
						SharedPreferences prefs = getContext().getSharedPreferences(PREFS_NAME, 0);
						SharedPreferences.Editor editor = prefs.edit();

						editor.putString(key, value);
						editor.apply();

						Log.v("replicate", key + "  " + value);
						break;


					}

					case 5: {


						OutputStream outToServer = null;
						try {
							outToServer = server.getOutputStream();
						} catch (Exception e) {
							Log.w(tag,e.toString());
						}
						DataOutputStream out = new DataOutputStream(outToServer);
						SharedPreferences prefs = getContext().getSharedPreferences(PREFS_NAME, 0);
						Map star = prefs.getAll();
						Set keys = star.keySet();
						String output = "";
						for (Iterator i = keys.iterator(); i.hasNext(); ) {
							String key = (String) i.next();
							String value = (String) star.get(key);
							//Log.v("P: ",key+" "+value);
							output = output + key + "$" + value + "$";
						}

						try {
							out.writeUTF(output);
						} catch (Exception e) {
							Log.w(tag,e.toString());
						}
						try {
							Thread.sleep(100);
						} catch (Exception e) {
							Log.w(tag,e.toString());
						}
						try {
							out.flush();
						} catch (Exception e) {
							Log.w(tag,e.toString());
						}
						break;

					}

					//else if (mode==4)
					case 4: {

						String selection = parts[1];
						OutputStream outToServer = null;
						try {
							outToServer = server.getOutputStream();
						} catch (Exception e) {
							Log.w(tag,e.toString());
						}
						DataOutputStream out = new DataOutputStream(outToServer);
						SharedPreferences prefs = getContext().getSharedPreferences(PREFS_NAME, 0);
						String value = prefs.getString(selection, "");
						try {
							out.writeUTF(value);
						} catch (Exception e) {
							Log.w(tag,e.toString());
						}
						try {
							Thread.sleep(100);
						} catch (Exception e) {
							Log.w(tag,e.toString());
						}
						try {
							out.flush();
						} catch (Exception e) {
							Log.w(tag,e.toString());
						}
						break;

					}
					//else if (mode==5)
					case 3: {
						String key = parts[1];
						String value = parts[2];


						SharedPreferences prefs = getContext().getSharedPreferences(PREFS_NAME, 0);
						SharedPreferences.Editor editor = prefs.edit();

						editor.putString(key, value);
						editor.apply();
						OutputStream outToServer = null;
						try {
							outToServer = server.getOutputStream();
						} catch (Exception e) {
							Log.w(tag,e.toString());
						}
						DataOutputStream out = new DataOutputStream(outToServer);
						try {
							out.writeUTF("OK");
						} catch (Exception e) {
							Log.w(tag,e.toString());
						}
						try {
							Thread.sleep(100);
						} catch (Exception e) {
							Log.w(tag,e.toString());
						}
						try {
							out.flush();
						} catch (Exception e) {
							Log.w(tag,e.toString());
						}

						// TODO Auto-generated method stub
						Log.v("insert", key + "  " + value);

						//For replicas:
						String[] succ = getsucc(srcPort);
						Socket socket = null;
						int i = 0;

						Iterator<Integer> mItr=mapSet.iterator();
						//while(mItr.hasNext()){
						while(i<2){
							String remoteport = succ[i];

							try {
								socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
										Integer.parseInt(remoteport));
							} catch (Exception e) {
								Log.w(tag,e.toString());
							}


							String select =  key + "$" + value;
							int mod= 31;
							String msgToSend = null;
							StringBuilder tempBuilder=new StringBuilder().append(Integer.toString(mod)+"$").append(select);
							msgToSend=tempBuilder.toString();

							outToServer = null;


							try {
								outToServer = socket.getOutputStream();
							} catch (Exception e) {
								Log.w(tag,e.toString());
							}


							out = new DataOutputStream(outToServer);

							try {
								out.writeUTF(msgToSend);
							} catch (Exception e) {
								Log.w(tag,e.toString());
							}
							try {
								Thread.sleep(30);
							} catch (Exception e) {
								Log.w(tag,e.toString());
							}
							try {
								out.flush();
							} catch (Exception e) {
								Log.w(tag,e.toString());
							}


							i++;
						}
						break;
					}


					case 6: {
						SharedPreferences prefs = getContext().getSharedPreferences(PREFS_NAME, 0);
						prefs.edit().clear().apply();
						break;
					}


				}
			}
		}

	}


	private class ClientTask extends AsyncTask<String, Void, Void> {


		@Override
		protected Void doInBackground(String... msgs) {

			String[] parts = msgs[0].split("\\$");
			int mode = Integer.parseInt(parts[0]);



			if(mode==1)
			{

				recover(srcPort);
			}

			else if(mode==2)
			{
				String key=parts[1];
				String value = parts[2];
				Socket socket = null;
				String remoteport = parts[3];

				try {
					socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt(remoteport));
				} catch (Exception e) {
					Log.w(tag,e.toString());
				}


				String selection =  key + "$" + value;
				int mod= 3;
				String msgToSend = null;
				StringBuilder tempBuilder=new StringBuilder().append(Integer.toString(mod)+"$").append(selection);
				msgToSend=tempBuilder.toString();

				OutputStream outToServer = null;
				DataInputStream in = null;

				try {
					in = new DataInputStream(socket.getInputStream());
				} catch (Exception e) {
					Log.w(tag,e.toString());
				}


				if (socket != null) {
					try {
						outToServer = socket.getOutputStream();
					} catch (Exception e) {
						Log.w(tag,e.toString());
					}
				}

				else
				{
					Log.v("Failure Found:","insert");
				}




				DataOutputStream out = new DataOutputStream(outToServer);
				try {
					out.writeUTF(msgToSend);
				} catch (Exception e) {
					Log.w(tag,e.toString());
				}
				try {
					Thread.sleep(200);
				} catch (Exception e) {
					Log.w(tag,e.toString());
				}
				try {
					out.flush();
				} catch (Exception e) {
					Log.w(tag,e.toString());
				}

				String ack = null;
				try {
					ack = in.readUTF();
				} catch (Exception e) {

					Log.v("Failure detected at ",remoteport);
					String[] succ = getsucc(remoteport);
					socket = null;
					int i=0;
					Iterator<Integer> mItr=mapSet.iterator();

					while(i<2){
						remoteport = succ[i];

						try {
							socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
									Integer.parseInt(remoteport));
						} catch (Exception e1) {
							e1.printStackTrace();
						}


						String select =  key + "$" + value;
						int md= 31;
						msgToSend = "";
						tempBuilder=new StringBuilder().append(Integer.toString(md)+"$").append(select);
						msgToSend=tempBuilder.toString();

						outToServer = null;


						try {
							outToServer = socket.getOutputStream();
						} catch (Exception e2) {
							e2.printStackTrace();
						}


						out = new DataOutputStream(outToServer);

						try {
							out.writeUTF(msgToSend);
						} catch (Exception e3) {
							e3.printStackTrace();
						}
						try {
							Thread.sleep(200);
						} catch (Exception e4) {
							e4.printStackTrace();
						}
						try {
							out.flush();
						} catch (Exception e5) {
							e5.printStackTrace();
						}
						i++;


					}
				}



			insertAv="0";


			}


			return null;
		}

		private void recover(String port)
		{


			String succ[]=getsucc(port);
			String s=succ[0];
			String pred[]=getpred(port);
			String p1=pred[0];
			String p2=pred[1];
			String[] rports={s,p1,p2};
			String[] tempoutput=new String[3];

			for (int i=0;i<rports.length;i++)
			{
				Socket socket = null;
				String remoteport = rports[i];

				try {
					socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt(remoteport));
				} catch (Exception e) {
					Log.v("Weird Exception at:",remoteport);
					Log.w(tag,e.toString());
				}


				int mod= 5;
				String msgToSend = null;
				StringBuilder tempBuilder=new StringBuilder().append(Integer.toString(mod)+"$").append("*");
				msgToSend=tempBuilder.toString();
				OutputStream outToServer = null;


				try {
					outToServer = socket.getOutputStream();
				} catch (Exception e) {
					Log.w(tag,e.toString());
				}


				DataOutputStream out = new DataOutputStream(outToServer);

				try {
					out.writeUTF(msgToSend);
				} catch (Exception e) {
					Log.w(tag,e.toString());
				}
				try {
					Thread.sleep(30);
				} catch (Exception e) {
					Log.w(tag,e.toString());
				}
				try {
					out.flush();
				} catch (Exception e) {
					Log.w(tag,e.toString());
				}


				DataInputStream in = null;

				try {
					in = new DataInputStream(socket.getInputStream());
				} catch (Exception e) {
					Log.w(tag,e.toString());
				}


				String op = null;
				try {
					op = in.readUTF();
				} catch (Exception e) {
					Log.w(tag,e.toString());
				}
				tempoutput[i]=op;


			}

			Map<String,String> smap=getstar(tempoutput[0]);
			Map<String,String> p1map=getstar(tempoutput[1]);
			Map<String,String> p2map=getstar(tempoutput[2]);

			SharedPreferences prefs = getContext().getSharedPreferences(PREFS_NAME, 0);
			SharedPreferences.Editor editor = prefs.edit();

			Set keys = smap.keySet();
			for (Iterator i = keys.iterator(); i.hasNext(); ) {
				String key = (String) i.next();
				String value = smap.get(key);
				String hash=null;
				try {
					hash = genHash(key);
				} catch (NoSuchAlgorithmException e) {
					Log.w(tag,e.toString());
				}
				int portInt=hashToPort(hash);
				String tport=Integer.toString(portInt);
				if(tport.equals(port)) {
					editor.putString(key, value);
					editor.apply();
				}


			}

			keys = p1map.keySet();
			for (Iterator i = keys.iterator(); i.hasNext(); ) {
				String key = (String) i.next();
				String value = p1map.get(key);
				String hash=null;
				try {
					hash = genHash(key);
				} catch (NoSuchAlgorithmException e) {
					Log.w(tag,e.toString());
				}
				int portInt=hashToPort(hash);
				String tport=Integer.toString(portInt);

				if(tport.equals(p1)) {
					editor.putString(key, value);
					editor.apply();
				}


			}

			keys = p2map.keySet();

			Iterator i = keys.iterator();
			while ( i.hasNext()) {
				String key = (String) i.next();
				String value = p2map.get(key);
				String hash=null;
				try {
					hash = genHash(key);
				} catch (NoSuchAlgorithmException e) {
					Log.w(tag,e.toString());
				}
				int portInt=hashToPort(hash);
				String tport=Integer.toString(portInt);

				if(tport.equals(p2)) {
					editor.putString(key, value);
					editor.apply();
				}
			}
			recovering=false;
			reboot="0";

		}

	}





	@Override
	public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs, String sortOrder)
	{
		Log.v("Query request:",selection);


		try {
			Thread.sleep(400);
		} catch (Exception e) {
			Log.w(tag,e.toString());
		}
		while(insertAv.equals("1"))
		{

			Log.v("Waiting in","query for insert");
		}


		while(recovering)
		{

			Log.v("Waiting in","query for recover");
		}









		SharedPreferences prefs = getContext().getSharedPreferences(PREFS_NAME, 0);

		if (selection.equals("@")) {
			Map localmap = prefs.getAll();
			String cnames[] = {"key", "value"};
			MatrixCursor matrixCursor = new MatrixCursor(cnames, 2);
			Set keys = localmap.keySet();
			Iterator i = keys.iterator();
			while ( i.hasNext()) {
				String key = (String) i.next();
				String value = (String) localmap.get(key);
				String keyvalue[] = {key, value};
				matrixCursor.addRow(keyvalue);

			}
			return matrixCursor;

		}
		else if(selection.equals("*"))
		{
			String Output ="";
			for (int i=0;i<ports.length;i++)
			{
				Socket socket = null;

				int temp_port = ports[i];
				String remoteport=Integer.toString(temp_port);

				try {
					socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt(remoteport));
				} catch (Exception e) {
					Log.w(tag,e.toString());
				}

				int mod= 5;
				String msgToSend = null;
				StringBuilder tempBuilder=new StringBuilder().append(Integer.toString(mod)+"$").append(selection);
				msgToSend=tempBuilder.toString();
				OutputStream outToServer = null;

				try {
					outToServer = socket.getOutputStream();
				} catch (Exception e) {
					Log.w(tag,e.toString());
				}


				DataOutputStream out = new DataOutputStream(outToServer);

				try {
					out.writeUTF(msgToSend);
				} catch (Exception e) {
					Log.w(tag,e.toString());
				}
				try {
					Thread.sleep(100);
				} catch (Exception e) {
					Log.w(tag,e.toString());
				}
				try {
					out.flush();
				} catch (Exception e) {
					Log.w(tag,e.toString());
				}


				DataInputStream in = null;

				try {
					in = new DataInputStream(socket.getInputStream());
				} catch (Exception e) {
					Log.w(tag,e.toString());
				}


				String output = null;
				try {
					output = in.readUTF();
					Output=Output+output;
				} catch (Exception e) {
					Log.w(tag,e.toString());
				}



			}
			Map<String,String> star = getstar(Output);
			String cnames[] = {"key", "value"};
			MatrixCursor matrixCursor = new MatrixCursor(cnames, 2);
			Set keys = star.keySet();
			Iterator i = keys.iterator();
			while ( i.hasNext()) {

				String key = (String) i.next();
				String value = (String) star.get(key);
				String keyvalue[] = {key, value};
				matrixCursor.addRow(keyvalue);

			}

			return matrixCursor;

		}
		else {
			String key= selection;
			String hash = null;

			try {
				hash = genHash(key);
			} catch (NoSuchAlgorithmException e) {
				Log.w(tag,e.toString());
			}
			int portInt=hashToPort(hash);
			String port_r=Integer.toString(portInt);

			if(port_r.equals(srcPort))
			{
				String value = prefs.getString(selection, "");
				String cnames[] = {"key", "value"};
				MatrixCursor matrixCursor = new MatrixCursor(cnames, 2);
				String keyvalue[] = {selection, value};
				matrixCursor.addRow(keyvalue);

				return matrixCursor;
			}
			else
			{
				Socket socket = null;
				String succ[]=getsucc(port_r);

				String remoteport = port_r;
				Log.v("Successor port",succ[1]);


				try {
					socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt(remoteport));
				} catch (Exception e) {
					Log.w(tag,e.toString());
				}


				int mod= 4;
				String msgToSend = null;
				StringBuilder tempBuilder=new StringBuilder().append(Integer.toString(mod)+"$").append(selection);
				msgToSend=tempBuilder.toString();
				OutputStream outToServer = null;

				try {
					outToServer = socket.getOutputStream();
				} catch (Exception e2) {
					Log.v("Query at "+succ[1],"failed 1");
					e2.printStackTrace();
				}
				try {
					DataOutputStream out = new DataOutputStream(outToServer);

					out.writeUTF(msgToSend);
					Thread.sleep(100);
					out.flush();
				} catch (Exception e3) {
					Log.v("Query at "+succ[1],"failed 2");
					e3.printStackTrace();
				}


				DataInputStream in = null;
				try {
					in = new DataInputStream(socket.getInputStream());
				} catch (Exception e) {
					Log.v("Query at "+succ[1],"failed 3");
					Log.w(tag,e.toString());
				}
				String output = null;
				try {
					output = in.readUTF();
					if(output==null||output.equals(""))
					{
						Log.v("Query at "+succ[1],"failed 4");

						output = querybackup(succ[0],selection,succ[1]);
					}
					Log.v("Retrieved "+output,"from "+succ[1]);
				} catch (Exception e) {


					Log.v("Query at "+succ[1],"failed 4");

					output = querybackup(succ[0],selection,port_r);
				}
				counter=counter+1;

				String cnames[] = {"key", "value"};
				MatrixCursor matrixCursor = new MatrixCursor(cnames, 2);
				String keyvalue[] = {selection, output};
				matrixCursor.addRow(keyvalue);
				return matrixCursor;
			}

		}



	}


	@Override
	public int update(Uri uri, ContentValues values, String selection,
					  String[] selectionArgs) {

		return 0;
	}




	private String genHash(String input) throws NoSuchAlgorithmException {
		MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
		byte[] sha1Hash = sha1.digest(input.getBytes());
		Formatter formatter = new Formatter();
		for (byte b : sha1Hash) {
			formatter.format("%02x", b);
		}
		return formatter.toString();
	}


	private String[] gethashtable(String[] a)
	{
		String output[]=new String[a.length];
		for(int i=0;i<a.length;i++)
		{

			try {
				output[i]=genHash(Integer.toString(Integer.parseInt(a[i]) / 2));
			} catch (NoSuchAlgorithmException e) {
				Log.w(tag,e.toString());
			}


		}

		return output;
	}

	private int hashToPort(String hkey)
	{
		int port_r=0;
		boolean portExists = false;

		for (int i=0;i<ports.length;i++)
		{

			if(hashtable[i].compareTo(hkey)>0)
			{

				port_r=ports[i];
				portExists = true;
				break;
			}
		}

		if(portExists==false)
		{
			port_r=ports[0];
		}


		return port_r;
	}


	private Map getstar(String s)
	{
		Map<String,String> star=new HashMap<String, String>();

		String[] parts = s.split("\\$");
		int n = parts.length/2;
		for(int i=0;i<parts.length;i++)
		{
			star.put(parts[i],parts[i+1]);
			i++;
		}

		return star;
	}

	private String[] getsucc(String port)
	{
		String succ[]=new String[2];
		int remPort=Integer.parseInt(port);
		switch (remPort){
			case 11124:
				succ[0]="11112";succ[1]="11108";
				break;
			case 11112:
				succ[0]="11108";succ[1]="11116";
				break;
			case 11108:
				succ[0]="11116";succ[1]="11120";
				break;
			case 11116:
				succ[0]="11120";succ[1]="11124";
				break;
			case 11120:
				succ[0]="11124";succ[1]="11112";
				break;
		}


		return succ;

	}

	private String[] getpred(String port)
	{
		String pred[]=new String[2];
		int remPort=Integer.parseInt(port);
		switch (remPort){
			case 11124:
				pred[0]="11120";pred[1]="11116";
				break;
			case 11112:
				pred[0]="11124";pred[1]="11120";
				break;
			case 11108:
				pred[0]="11112";pred[1]="11124";;
				break;
			case 11116:
				pred[0]="11108";pred[1]="11112";
				break;
			case 11120:
				pred[0]="11116";pred[1]="11108";
				break;
		}

		return pred;

	}

	private String querybackup(String remoteport,String selection,String ogport)
	{
		Socket socket = null;
		try {
			socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
					Integer.parseInt(remoteport));
		} catch (Exception e) {
			Log.w(tag,e.toString());
		}


		int mod= 4;
		String msgToSend = null;
		StringBuilder tempBuilder=new StringBuilder().append(Integer.toString(mod)+"$").append(selection);
		msgToSend=tempBuilder.toString();
		OutputStream outToServer = null;

		try {
			outToServer = socket.getOutputStream();
		} catch (Exception e2) {

			e2.printStackTrace();
		}
		try {
			DataOutputStream out = new DataOutputStream(outToServer);

			out.writeUTF(msgToSend);

			out.flush();
		} catch (Exception e3) {
			e3.printStackTrace();
		}


		DataInputStream in = null;
		try {
			in = new DataInputStream(socket.getInputStream());
		} catch (Exception e) {
			Log.w(tag,e.toString());
		}
		String output = null;
		try {
			output = in.readUTF();
			if(output==null||output.equals(""))
			{

				output = querybackup(ogport,selection,ogport);
			}
		} catch (Exception e) {


			//Log.w(tag,e.toString());
			Log.w(tag,remoteport+"failed Query 4");
			output = querybackup(ogport,selection,ogport);

		}

		return output;
	}






}
