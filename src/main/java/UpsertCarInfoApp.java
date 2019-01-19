import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.concurrent.ExecutionException;

/**
 * Created by W8616 on 2017/6/23.
 */
public class UpsertCarInfoApp {
    public static void main(String[] args) throws Exception {
        TransportClient clinet = new PreBuiltTransportClient(Settings.builder().put("cluster.name", "elasticsearch").build()).addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("117.50.3.9"), Integer.valueOf(9300)));
        String id = Long.toString(new Date().getTime());
        IndexRequest indexRequest = new IndexRequest("wb_set_subtext_logs","wb_set_subtext",id)
                .source(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("loginName", "testName")
                        .field("timestamp", "2018-02-03 03:03:03")
                        .field("isViolation","true")
                        .field("violation", "1")
                        .field("category", "2")
                        .field("gifId", "123456")
                        .field("label", "label")
                        .field("status", "true")
                        .endObject());
        UpdateRequest updateRequest = new UpdateRequest("wb_set_subtext_logs","wb_set_subtext",id)
                .doc(XContentFactory.jsonBuilder()
                .startObject()
                        .field("status","false")
                .endObject()).upsert(indexRequest);
//
        UpdateResponse updateResponse = clinet.update(updateRequest).get();
//        IndexResponse indexResponse = clinet.index(indexRequest).get();
        System.out.println(updateResponse.getResult().getOp());//获取操作
//        System.out.println(indexResponse.getResult().getOp());//获取操作
        System.out.println(updateResponse.getVersion());
//        System.out.println(indexResponse.getVersion());
//
        System.out.println(updateResponse.toString());
    }
//    public static void main(String[] args) throws UnknownHostException {
//        TransportClient clinet = new PreBuiltTransportClient(Settings.builder().put("cluster.name", "elasticsearch").build()).addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("117.50.3.9"), Integer.valueOf(9300)));

//        addIndex(clinet);
//    }
    public static void addIndex(TransportClient client){
        String id = Long.toString(new Date().getTime());
        try {
            IndexRequest indexRequest = new IndexRequest("wb_set_subtext_logs","wb_set_subtext","1517630181139")
                    .source(XContentFactory.jsonBuilder()
                            .startObject()
                            .field("loginName", "testName")
                            .field("timestamp", "2018-02-03 03:03:03")
                            .field("isViolation","true")
                            .field("violation", "1")
                            .field("category", "2")
                            .field("gifId", "1341")
                            .field("label", "label")
                            .field("status", "true")
                            .endObject());
            IndexResponse indexResponse = client.index(indexRequest).get();
            System.out.println(indexResponse.getResult().getOp());
            System.out.println(indexResponse.getVersion());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void updates(TransportClient client){
        BulkRequestBuilder bulkRequest = client.prepareBulk();
        UpdateRequestBuilder updateRequestBuilder = null;
        try {
            UpdateRequestBuilder pi = client.prepareUpdate("wb_set_subtext_logs", "wb_set_subtext",Long.toString(new Date().getTime()));
            updateRequestBuilder = pi
                    .setDoc(XContentFactory.jsonBuilder()
                            .startObject()
                            .field("loginName", "testName")
                            .field("timestamp", "2018-02-03 03:03:03")
                            .field("isViolation","true")
                            .field("violation", "1")
                            .field("category", "2")
                            .field("gifId", "123456")
                            .field("label", "label")
                            .field("status", "true")
                            .endObject());
            bulkRequest.add(updateRequestBuilder);
        } catch (IOException e) {
            e.printStackTrace();
        }
        BulkResponse bulkResponse = bulkRequest.get();
        for(BulkItemResponse bulkItemResponse : bulkResponse.getItems()) {
            if(bulkItemResponse.getVersion()==-1){
            }
        }
    }
}
