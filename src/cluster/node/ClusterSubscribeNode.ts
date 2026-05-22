import { Node, NodeStatus } from "node-red";
import { BaseNode, BaseNodeConfig, NodeDescription, NodeManager, SourceUtility } from "@theotherwillembotha/node-red-plugincore";
import { Subscription } from "@nats-io/nats-core";
import { ClusterConfigNode } from "./ClusterConfigNode";
import { ClusterClient } from "../service/ClusterClient";

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
    private _subscriptions:     Subscription[] = [];
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

            if (removed) { /* nothing to clean up */ }

            this._unsubscribe();
            done();
        });
    }

    // ── Subscription management ──────────────────────────────────────────────

    private _subscribe(client: ClusterClient): void {
        if (this._subscriptions.length > 0) return; // already subscribed

        const nc = client.natsClient();
        if (!nc) return;

        const patterns = this._subjectPattern
            .split(",")
            .map(p => p.trim())
            .filter(p => p.length > 0);

        const node = this.node();
        for (const pattern of patterns) {
            const sub = nc.subscribe(client.buildSubscribePattern(pattern));
            this._subscriptions.push(sub);
            (async () => {
                for await (const msg of sub) {
                    try {
                        node.send({ payload: JSON.parse(msg.string()), topic: msg.subject });
                    } catch (_) {
                        node.send({ payload: msg.string(), topic: msg.subject });
                    }
                }
            })();
        }

        const count = this._subscriptions.length;
        this.node().status(count > 1
            ? { fill: "green", shape: "dot", text: `subscribed (${count})` }
            : subscribedStatus
        );
    }

    private _unsubscribe(): void {
        this._subscriptions.forEach(sub => sub.unsubscribe());
        this._subscriptions = [];
    }

}
