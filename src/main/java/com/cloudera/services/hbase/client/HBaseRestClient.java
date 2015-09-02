package com.cloudera.services.hbase.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.rest.client.Client;
import org.apache.hadoop.hbase.rest.client.Cluster;
import org.apache.hadoop.hbase.rest.client.RemoteHTable;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseRestClient {
	
	private Cluster cluster;
	private Client client;
	private RemoteHTable table;
	
	public HBaseRestClient(String restHost, int port, String tableName) {
		cluster = new Cluster();
		cluster.add(restHost, port);
		client = new Client(cluster);
		table = new RemoteHTable(client, tableName);
	}

/*	public static void main(String[] args) throws IOException {
        HBaseRestExample restExample = new HBaseRestExample("myhbase-rest-client.com", 20550, "pwdata");
		//get.addColumn(Bytes.toBytes("c"), Bytes.toBytes("batch_id"));
		List<String> gid_list = new ArrayList<String>();
		gid_list.add("00502");
		gid_list.add("01002");
		
		restExample.lookupHbase(gid_list);
	} */
	
	public void lookupHbase(List<String> gid_list) throws IOException {
		if(gid_list.size() == 0)
			return;
		
		List<Get> gets = new ArrayList<Get>();
		for(String gid : gid_list) {
		  Get get = new Get(Bytes.toBytes(gid));
		  gets.add(get);
		}

		Result[] results = table.get(gets);
		
		for(Result result : results) {
		   System.out.println("Got result1: " + result);
		   dumpResult(result);
		}
	}

	private void dumpResult(Result result) {
		for (Cell cell : result.rawCells()) {
			System.out.println("Cell: "
					+ cell
					+ ", Value: "
					+ Bytes.toString(cell.getValueArray(),
							cell.getValueOffset(), cell.getValueLength()));
		}
	}
}
