package com.example.esas.nativescript;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.BatchPolicy;
import com.example.esas.aerospike.AerospikeConnection;
import org.elasticsearch.common.base.Strings;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.script.AbstractSearchScript;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.NativeScriptFactory;
import org.elasticsearch.script.ScriptException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collector;
import java.util.stream.Collectors;

/**
 * Created by santanu.s on 19/08/15.
 */
public class CheckScript extends AbstractSearchScript {

    public static class Factory implements NativeScriptFactory {

        private AerospikeClient aerospikeClient;

        public Factory() {
            this.aerospikeClient = AerospikeConnection.INSTANCE.getAerospikeClient();
        }

        @Override
        public ExecutableScript newScript(Map<String, Object> params) {
            String fieldName = params == null ? null : XContentMapValues.nodeStringValue(params.get("field"), null);
            if (fieldName == null) {
                throw new ScriptException("Missing the field parameter");
            }
            String value = XContentMapValues.nodeStringValue(params.get("value"), null);
            if (value == null) {
                throw new ScriptException("Missing the value parameter");
            }
            return new CheckScript(fieldName, value, aerospikeClient);
        }
    }

    private final String field;
    private final String value;
    private AerospikeClient aerospikeClient;

    public CheckScript(String field, String value, AerospikeClient aerospikeClient) {
        this.field = field;
        this.value = value;
        this.aerospikeClient = aerospikeClient;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Object run() {

        List<Key> keyList = Lists.newArrayList();

        //The following handles array fields as well
        //If the field is known to be not array, just go get(0)
        //and move accordingly with one key

        ((ScriptDocValues<String>) doc()
                                    .get(field))
                                    .stream()
                                    .forEach((String value) -> {
                                        System.out.println("Key: " + value);
                                        keyList.add(new Key("test", "test", value));
                                    });
        Key keys[] = keyList.toArray(new Key[keyList.size()]);
        BatchPolicy policy = new BatchPolicy();
        Record[] records = aerospikeClient.get(policy, keys, "status");
        for(Record record : records) {
            final String receivedValue = record.getString("status");
            if(Strings.isNullOrEmpty(receivedValue)) {
                System.out.println("No AS value found for: " + keys[0].userKey.toString());
                return false;
            }
            else {
                if(!receivedValue.equals(value)) {
                    System.out.println("Value mismatch: Expected: " + value + " Found: " + receivedValue);
                    return false;
                }
            }
        }
        return true;
    }
}
