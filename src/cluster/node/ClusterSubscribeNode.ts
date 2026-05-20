import { Node, NodeStatus } from "node-red";
import { BaseNode, BaseNodeConfig, NodeDescription, NodeManager, SourceUtility } from "@theotherwillembotha/node-red-plugincore";
import { Subscription } from "@nats-io/nats-core";
import { ClusterConfigNode } from "./ClusterConfigNode";
import { ClusterClient } from "../service/ClusterClient";
import * as zookeeper from "node-zookeeper-client";

export interface ClusterSubscribeNodeConfig extends BaseNodeConfig {
    clusterConfig:  string;
    subjectPattern: string;
}

const subscribedStatus:   NodeStatus = { fill: "green",  shape: "dot",  text: "subscribed"              };
const disconnectedStatus: NodeStatus = { fill: "red",    shape: "ring", text: "disconnected"             };
const connectingStatus:   NodeStatus = { fill: "yellow", shape: "ring", text: "waiting for connection..." };

@NodeDescription({
    id:           "ClusterSubscribeNode",
    name:         "Cluster Subscribe Node",
    group:        "cluster",
    sourceFile:   SourceUtility.getSourcePath("/build/", "/src/") + "ClusterSubscribeNode.html",
    package:      "@theotherwillembotha/node-red-cluster",
    dependencies: [ ClusterConfigNode ],
    tags:         [ "Cluster" ]
})
export class ClusterSubscribeNode extends BaseNode<ClusterSubscribeNodeConfig> {

    private _configNode:        ClusterConfigNode;
    private _subjectPattern:    string;
    private _subscription:      Subscription | null = null;
    private _unsubscribeStatus: (() => void) | null = null;

    constructor(node: Node, config: ClusterSubscribeNodeConfig) {
        super(node, config);

        this._configNode     = (NodeManager.RED.nodes.getNode(config.clusterConfig) as any).node();
        this._subjectPattern = config.subjectPattern;

        const client = this._configNode.getClient();
        if (client) {
            this._unsubscribeStatus = client.onStatusChange((status) => {
                if (status === "connected") {
                    this._subscribe(client);
                } else {
                    this._unsubscribe();
                    this.node().status(disconnectedStatus);
                }
            });

            if (client.status() === "connected") {
                this._subscribe(client);
            } else {
                this.node().status(connectingStatus);
            }
        } else {
            this.node().status(connectingStatus);
        }
    }

    protected onInit(): void {
        this.node().on("close", (removed: boolean, done: () => void) => {
            this._unsubscribeStatus?.();
            this._unsubscribeStatus = null;

            if (removed) {
                const client = this._configNode.getClient();
                if (client) this._removeSubscriptionRecord(client);
            }

            this._unsubscribe();
            done();
        });
    }

    // ── Subscription management ──────────────────────────────────────────────

    private _subscribe(client: ClusterClient): void {
        if (this._subscription) return; // already subscribed

        const nc      = client.natsClient();
        if (!nc) return;

        const pattern = client.buildSubscribePattern(this._subjectPattern);
        this._subscription = nc.subscribe(pattern);

        // Drain the async iterator in the background
        const sub  = this._subscription;
        const node = this.node();
        (async () => {
            for await (const msg of sub) {
                try {
                    const payload = JSON.parse(msg.string());
                    node.send({ payload, topic: msg.subject });
                } catch (_) {
                    // Message data is not JSON — forward raw string
                    node.send({ payload: msg.string(), topic: msg.subject });
                }
            }
        })();

        this.node().status(subscribedStatus);
        this._writeSubscriptionRecord(client);
    }

    private _unsubscribe(): void {
        if (this._subscription) {
            this._subscription.unsubscribe();
            this._subscription = null;
        }
    }

    // ── ZK subscription record ───────────────────────────────────────────────

    private _subscriptionPath(client: ClusterClient): string {
        const { zkRootPath, instanceId } = client.params();
        return `${zkRootPath}/subscriptions/${instanceId}/${this._subjectPattern}`;
    }

    private _writeSubscriptionRecord(client: ClusterClient): void {
        const zk = client.zkClient();
        if (!zk) return;

        const path = this._subscriptionPath(client);
        const data = Buffer.from(JSON.stringify({
            instanceId: client.params().instanceId,
            role:       client.params().role
        }));
        const parentPath = path.substring(0, path.lastIndexOf("/"));

        zk.mkdirp(parentPath, (mkErr) => {
            if (mkErr) return;
            zk.create(path, data, zookeeper.CreateMode.PERSISTENT, (createErr) => {
                if (createErr && (createErr as zookeeper.Exception).getCode?.() === zookeeper.Exception.NODE_EXISTS) {
                    zk.setData(path, data, -1, () => { /* best-effort update */ });
                }
            });
        });
    }

    private _removeSubscriptionRecord(client: ClusterClient): void {
        const zk = client.zkClient();
        if (!zk) return;
        zk.remove(this._subscriptionPath(client), -1, () => { /* best-effort */ });
    }
}
