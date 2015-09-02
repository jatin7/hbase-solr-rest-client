package com.cloudera.services.hbase.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.XMLResponseParser;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;

public class SolrRestClient {

	public static void main(String[] args) throws SolrServerException,
			IOException {
		CloudSolrClient solrClient = new CloudSolrClient(ConfigUtils.serviceProps.getProperty("solr_zk_url"));
		solrClient.setDefaultCollection(ConfigUtils.serviceProps.getProperty("solr_collection_name"));
		solrClient.setParser(new XMLResponseParser());
		SolrQuery query = new SolrQuery();
		query.set("q", "*:*");
		query.set("qt", "/select");
		query.set("collection", ConfigUtils.serviceProps.getProperty("solr_collection_name"));
		query.setStart(0);
		query.setRows(10000);

		// SolrQuery query = new SolrQuery();
		// query.setQuery( "*:*" );
		// query.addSort( "price", SolrQuery.ORDER.asc );

		long time1 = System.currentTimeMillis();

		QueryResponse response = solrClient.query(query);

		HBaseRestClient restExample = new HBaseRestClient(
				ConfigUtils.serviceProps.getProperty("hbase_rest_server"),
				Integer.valueOf(ConfigUtils.serviceProps.getProperty("hbase_rest_server_port")),
				ConfigUtils.serviceProps.getProperty("hbase_table_name"));
		List<String> gid_list = new ArrayList<String>();
		SolrDocumentList resultList = response.getResults();
		System.out.println(resultList.size());
		int i = 0;
		for (SolrDocument doc : resultList) {
			i++;
			for (String solrField : doc.getFieldNames()) {
				System.out.println("solrField " + solrField + " -- value "+ doc.getFieldValue(solrField));
				if (solrField.equals("id"))
					gid_list.add(doc.getFieldValue(solrField).toString());
			}
			if (i % 100 == 0) {
				restExample.lookupHbase(gid_list);
				gid_list = new ArrayList<String>();
			}
		}

		restExample.lookupHbase(gid_list);

		long time2 = System.currentTimeMillis();

		System.out.println("time taken " + (time2 - time1) / 1000);
		solrClient.close();
	}
}
