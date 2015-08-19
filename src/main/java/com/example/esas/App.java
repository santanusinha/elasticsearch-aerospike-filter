package com.example.esas;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.policy.CommitLevel;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.example.esas.aerospike.AerospikeConnection;
import com.example.esas.nativescript.CheckScript;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.collect.ImmutableBiMap;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

public class App {
    public static void main(String[] args) {


        Settings settings = ImmutableSettings.builder()
                .put("script.native.checker.type", CheckScript.Factory.class)
                .put("checker.aerospike.host", "172.28.128.3") //Settings for connecting from factory
                .put("checker.aerospike.port", 3000)
                .build();
        Node node = nodeBuilder()
                .settings(settings)
                .local(true)
                .build();

        node.start();

        node.client().prepareIndex("filter_test", "document")
                .setSource(
                        "{\"device_id\" : \"aa\", \"type\" : \"test1\" }"
                )
                .setRefresh(true)
                .execute()
                .actionGet();

        node.client().prepareIndex("filter_test", "document")
                .setSource(
                        "{\"device_id\" : \"ab\", \"type\" : \"test2\" }"
                )
                .setRefresh(true)
                .execute()
                .actionGet();

        node.client().prepareIndex("filter_test", "document")
                .setSource(
                        "{\"device_id\" : \"bb\", \"type\" : \"test2\" }"
                )
                .setRefresh(true)
                .execute()
                .actionGet();

        SearchResponse res = node.client().prepareSearch("filter_test")
                .setTypes("document")
                .setQuery(QueryBuilders.constantScoreQuery(
                        FilterBuilders.andFilter(
                                FilterBuilders.termFilter("type", "test2"),
                                FilterBuilders.scriptFilter("checker")
                                        .lang("native")
                                        .params(ImmutableBiMap.<String, Object>builder()
                                                .put("field", "device_id")
                                                .put("value", "disabled")
                                                .build())
                        )
                ))
                .execute()
                .actionGet();

        if(0 != res.getHits().totalHits()) {
            System.out.println("Somethings not right ..");
            return;
        }
        System.out.println("No record found as expected");
        //MUST BE DONE BEFORE SETTING UP THE CLIENT
        AerospikeConnection.INSTANCE.connect();
        WritePolicy writePolicy = new WritePolicy();
        writePolicy.commitLevel = CommitLevel.COMMIT_ALL;
        writePolicy.recordExistsAction = RecordExistsAction.UPDATE;
        Bin status = new Bin("status", "enabled");
        AerospikeConnection.INSTANCE.getAerospikeClient().put(writePolicy, new Key("test", "test", "bb"), status);
        res = node.client().prepareSearch("filter_test")
                .setTypes("document")
                .setQuery(QueryBuilders.constantScoreQuery(
                        FilterBuilders.andFilter(
                                FilterBuilders.termFilter("type", "test2"),
                                FilterBuilders.scriptFilter("checker")
                                        .lang("native")
                                        .params(ImmutableBiMap.<String, Object>builder()
                                                .put("field", "device_id")
                                                .put("value", "enabled")
                                                .build())
                                )
                ))
                .execute()
                .actionGet();

        if(0 == res.getHits().totalHits()) {
            System.out.println("Somethings not right .. Should be found");
            return;
        }
        System.out.println("Record found with key: " + res.getHits().getHits()[0].getSource().get("device_id"));
        node.stop();
        AerospikeConnection.INSTANCE.getAerospikeClient().close();
    }
}
