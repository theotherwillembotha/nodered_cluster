import { Node, NodeStatus } from "node-red";
import { ConfigNode, ConfigNodeConfig, NodeDescription, SourceUtility } from "@theotherwillembotha/node-red-plugincore";
import { ClusterService } from "../service/ClusterService";
import { ClusterClient, ClusterClientStatus } from "../service/ClusterClient";

export enum ClusterRole {
    Member   = "Member",
    Observer = "Observer"
}

export interface ClusterConfigNodeConfig extends ConfigNodeConfig {
    instanceId:  string;
    zkRootPath:  string;
    natsAddress: string;
    natsUser:    string;
    natsPass:    string;
    role:        ClusterRole;
}

function toNodeStatus(status: ClusterClientStatus, error?: Error): NodeStatus {
    switch (status) {
        case "connecting":    return { fill: "yellow", shape: "ring", text: "connecting..." };
        case "connected":     return { fill: "green",  shape: "dot",  text: "connected"    };
        case "disconnected":  return { fill: "red",    shape: "ring", text: "disconnected" };
        case "error":         return { fill: "red",    shape: "ring", text: error?.message ?? "error" };
        default:              return { fill: "grey",   shape: "ring", text: "idle"         };
    }
}

@NodeDescription({
    id:         "ClusterConfigNode",
    name:       "Cluster Config Node",
    group:      "config",
    sourceFile: SourceUtility.getSourcePath("/build/", "/src/") + "ClusterConfigNode.html",
    package:    "@theotherwillembotha/node-red-cluster",
    tags:       [ "Cluster" ]
})
export class ClusterConfigNode extends ConfigNode<ClusterConfigNodeConfig> {

    private _instanceId:  string;
    private _zkRootPath:  string;
    private _natsAddress: string;
    private _natsUser:    string;
    private _natsPass:    string;
    private _role:        ClusterRole;

    private _unsubscribeStatus: (() => void) | null = null;

    constructor(node: Node, config: ClusterConfigNodeConfig) {
        super(node, config);

        this._instanceId  = config.instanceId;
        this._zkRootPath  = config.zkRootPath  || "/nodered-cluster";
        this._natsAddress = config.natsAddress;
        this._natsUser    = config.natsUser    || "";
        this._natsPass    = config.natsPass    || "";
        this._role        = config.role        || ClusterRole.Member;

        const client = ClusterService.ensureClient(this.id(), {
            instanceId:  this._instanceId,
            zkRootPath:  this._zkRootPath,
            natsAddress: this._natsAddress,
            natsUser:    this._natsUser    || undefined,
            natsPass:    this._natsPass    || undefined,
            role:        this._role,
        });

        // Reflect the live client status on this node's status indicator.
        this.node().status(toNodeStatus(client.status()));
        this._unsubscribeStatus = client.onStatusChange((status, error) => {
            this.node().status(toNodeStatus(status, error));
        });
    }

    protected onInit(): void {
        this.node().on("close", (removed: boolean, done: () => void) => {
            this._unsubscribeStatus?.();
            this._unsubscribeStatus = null;

            if (removed) {
                // Node permanently deleted  tear down the cluster connection.
                ClusterService.removeClient(this.id());
            }
            done();
        });
    }

    // ── Accessors ────────────────────────────────────────────────────────────

    public instanceId():  string      { return this._instanceId;  }
    public zkRootPath():  string      { return this._zkRootPath;  }
    public natsAddress(): string      { return this._natsAddress; }
    public natsUser():    string      { return this._natsUser;    }
    public natsPass():    string      { return this._natsPass;    }
    public role():        ClusterRole { return this._role;        }

    public isObserver(): boolean {
        return this._role === ClusterRole.Observer;
    }

    public getClient(): ClusterClient | null {
        return ClusterService.getClient(this.id());
    }
}
