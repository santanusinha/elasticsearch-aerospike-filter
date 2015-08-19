package com.example.esas.aerospike;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.policy.ClientPolicy;

/**
 * Created by santanu.s on 19/08/15.
 */
public enum AerospikeConnection {
    INSTANCE;
    private AerospikeClient aerospikeClient;

    public void connect() {
        ClientPolicy clientPolicy = new ClientPolicy();
        clientPolicy.timeout = 5000;
        aerospikeClient = new AerospikeClient(clientPolicy, "172.28.128.3", 3000);
    }

    public AerospikeClient getAerospikeClient() {
        return aerospikeClient;
    }
}
