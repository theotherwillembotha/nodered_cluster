import { Node, NodeStatus } from "node-red";
import { BaseNode, BaseNodeConfig, NodeDescription, NodeManager, SourceUtility, onInput, Message } from "@theotherwillembotha/node-red-plugincore";
import { ClusterConfigNode } from "./ClusterConfigNode";
import { ClusterClient } from "../service/ClusterClient";

export enum DeliveryMode {
    Durable   = "Durable",
    Ephemeral = "Ephemeral"
}

export interface ClusterPublishNodeConfig extends BaseNodeConfig {
    clusterConfig: string;
    subject:       string;
    mode:          DeliveryMode;
    ttl:           number;
}

const observerStatus:      NodeStatus = { fill: "grey",   shape: "ring", text: "observer  publish disabled" };
const readyStatus:         NodeStatus = { fill: "green",  shape: "dot",  text: "ready"                       };
const notConnectedStatus:  NodeStatus = { fill: "yellow", shape: "ring", text: "waiting for connection..."   };

@NodeDescription({
    id:           "ClusterPublishNode",
    name:         "Cluster Publish Node",
    group:        "cluster",
    sourceFile:   SourceUtility.getSourcePath("/build/", "/src/") + "ClusterPublishNode.html",
    package:      "@theotherwillembotha/node-red-cluster",
    dependencies: [ ClusterConfigNode ],
    tags:         [ "Cluster" ]
})
export class ClusterPublishNode extends BaseNode<ClusterPublishNodeConfig> {

    private _configNode:        ClusterConfigNode;
    private _subject:           string;
    private _mode:              DeliveryMode;
    private _ttl:               number;
    private _unsubscribeStatus: (() => void) | null = null;

    constructor(node: Node, config: ClusterPublishNodeConfig) {
        super(node, config);

        this._configNode = (NodeManager.RED.nodes.getNode(config.clusterConfig) as any).node();
        this._subject    = config.subject;
        this._mode       = config.mode || DeliveryMode.Durable;
        this._ttl        = config.ttl  || 30;

        if (this._configNode.isObserver()) {
            this.node().status(observerStatus);
            return;
        }

        const client = this._configNode.getClient();
        if (client) {
            this.node().status(client.status() === "connected" ? readyStatus : notConnectedStatus);
            this._unsubscribeStatus = client.onStatusChange((status) => {
                if (status === "connected") {
                    this.node().status(readyStatus);
                    this._announceEndpoint(client);
                } else {
                    this.node().status(notConnectedStatus);
                }
            });

            if (client.status() === "connected") {
                this._announceEndpoint(client);
            }
        } else {
            this.node().status(notConnectedStatus);
        }
    }

    protected onInit(): void {
        this.node().on("close", (removed: boolean, done: () => void) => {
            this._unsubscribeStatus?.();
            this._unsubscribeStatus = null;

            if (removed) {
                const client = this._configNode.getClient();
                if (client) this._removeEndpoint(client);
            }
            done();
        });
    }

    @onInput()
    protected async messageReceived(message: Message): Promise<void> {
        if (this._configNode.isObserver()) return;

        const client = this._configNode.getClient();
        if (!client || client.status() !== "connected") {
            this.node().warn("ClusterPublish: not connected, message dropped");
            return;
        }

        const subject = client.buildPublishSubject(this._subject);
        const data    = JSON.stringify(message.payload);

        try {
            if (this._mode === DeliveryMode.Durable) {
                await client.jsPublish(subject, data);
            } else {
                client.corePublish(subject, data);
            }
        } catch (err: any) {
            this.node().error(`ClusterPublish: failed to publish: ${err.message}`, message);
            this.node().status({ fill: "red", shape: "ring", text: `publish error: ${err.message}` });
        }
    }

    // ── KV endpoint announcement ─────────────────────────────────────────────

    private _announceEndpoint(client: ClusterClient): void {
        client.announceEndpoint(this._subject, this._mode, this._ttl).catch(err => {
            this.node().warn(`ClusterPublish: failed to announce endpoint: ${err.message}`);
        });
    }

    private _removeEndpoint(client: ClusterClient): void {
        client.removeEndpoint(this._subject).catch(() => { /* best-effort */ });
    }
}
