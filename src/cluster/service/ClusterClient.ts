import { connect } from "@nats-io/transport-node";
import { NatsConnection } from "@nats-io/nats-core";
import { jetstream, jetstreamManager } from "@nats-io/jetstream";
import * as zookeeper from "node-zookeeper-client";
import { ClusterRole } from "../node/ClusterConfigNode";

export type ClusterClientParams = {
    instanceId:  string;
    zkAddress:   string;
    zkRootPath:  string;
    natsAddress: string;
    role:        ClusterRole;
};

export type ClusterClientStatus = "idle" | "connecting" | "nats-connected" | "connected" | "zk-disconnected" | "error";

type StatusListener = (status: ClusterClientStatus, error?: Error) => void;

export class ClusterClient {

    private _params:       ClusterClientParams;
    private _natsClient:   NatsConnection | null = null;
    private _zkClient:     zookeeper.Client | null = null;
    private _status:       ClusterClientStatus = "idle";
    private _statusListeners: StatusListener[] = [];
    private _streamName:   string | null = null;

    constructor(params: ClusterClientParams) {
        this._params = params;
    }

    // ── Lifecycle ────────────────────────────────────────────────────────────

    /**
     * Called by ClusterService when the config node is redeployed with changed settings.
     * Restarts the client only if the params actually changed.
     */
    public updateParams(params: ClusterClientParams): void {
        if (JSON.stringify(params) === JSON.stringify(this._params)) return;
        this._params = params;
        this._streamName = null;
        this.stop().then(() => this.start()).catch(err => {
            console.error("ClusterClient: restart after param change failed:", err);
        });
    }

    public async start(): Promise<void> {
        this._setStatus("connecting");

        this._natsClient = await connect({ servers: this._params.natsAddress });

        await this._connectZK();

        this._setStatus("connected");
    }

    public async stop(): Promise<void> {
        if (this._natsClient) {
            try { await this._natsClient.drain(); } catch (_) { /* ignore drain errors */ }
            this._natsClient = null;
        }
        if (this._zkClient) {
            this._zkClient.close();
            this._zkClient = null;
        }
        this._streamName = null;
        this._setStatus("idle");
    }

    // ── Accessors ────────────────────────────────────────────────────────────

    public natsClient(): NatsConnection | null   { return this._natsClient; }
    public zkClient():   zookeeper.Client | null { return this._zkClient;   }
    public status():     ClusterClientStatus     { return this._status;     }
    public params():     ClusterClientParams     { return this._params;     }

    // ── Subject helpers ──────────────────────────────────────────────────────

    /** `nodered-cluster.field-a.commands.opengate` */
    public buildPublishSubject(subject: string): string {
        return `${this.rootPrefix()}.${this._params.instanceId}.${subject}`;
    }

    /** `nodered-cluster.*.commands.opengate` */
    public buildSubscribePattern(subjectPattern: string): string {
        return `${this.rootPrefix()}.${subjectPattern}`;
    }

    /** Strip leading `/`, replace `/` with `.`  e.g. `/nodered-cluster` → `nodered-cluster` */
    public rootPrefix(): string {
        return this._params.zkRootPath.replace(/^\//, "").replace(/\//g, ".");
    }

    // ── Stream management ────────────────────────────────────────────────────

    /**
     * Ensures the JetStream stream for this cluster exists.
     * Safe to call multiple times — result is cached after first creation.
     * Returns the stream name.
     */
    public async ensureStream(): Promise<string> {
        if (this._streamName) return this._streamName;

        const nc      = this._natsClient!;
        const jsm     = await jetstreamManager(nc);
        const prefix  = this.rootPrefix();
        const name    = prefix.toUpperCase().replace(/[.\-\/]/g, "_");

        try {
            await jsm.streams.info(name);
        } catch (_) {
            // Stream does not exist — create it
            await jsm.streams.add({ name, subjects: [`${prefix}.>`] });
        }

        this._streamName = name;
        return name;
    }

    /** Publish to JetStream (durable delivery). Ensures stream exists first. */
    public async jsPublish(subject: string, data: string): Promise<void> {
        await this.ensureStream();
        const js = jetstream(this._natsClient!);
        await js.publish(subject, data);
    }

    /** Publish via core NATS (ephemeral — fire and forget). */
    public corePublish(subject: string, data: string): void {
        this._natsClient!.publish(subject, data);
    }

    // ── Status subscriptions ─────────────────────────────────────────────────

    public onStatusChange(listener: StatusListener): () => void {
        this._statusListeners.push(listener);
        return () => {
            this._statusListeners = this._statusListeners.filter(l => l !== listener);
        };
    }

    // ── Internals ────────────────────────────────────────────────────────────

    private _setStatus(status: ClusterClientStatus, error?: Error): void {
        this._status = status;
        this._statusListeners.forEach(l => l(status, error));
    }

    private _connectZK(): Promise<void> {
        return new Promise((resolve, reject) => {
            const client = zookeeper.createClient(this._params.zkAddress);
            this._zkClient = client;

            client.once("connected", () => {
                this._registerInstance()
                    .then(resolve)
                    .catch(reject);
            });

            client.on("disconnected", () => {
                this._setStatus("zk-disconnected");
            });

            client.connect();
        });
    }

    private _registerInstance(): Promise<void> {
        return new Promise((resolve, reject) => {
            const instancesPath = `${this._params.zkRootPath}/instances`;
            const instancePath  = `${instancesPath}/${this._params.instanceId}`;
            const data = Buffer.from(JSON.stringify({
                natsAddress: this._params.natsAddress,
                role:        this._params.role
            }));

            this._zkClient!.mkdirp(instancesPath, (mkErr) => {
                if (mkErr) return reject(mkErr);

                this._zkClient!.create(instancePath, data, zookeeper.CreateMode.EPHEMERAL, (createErr) => {
                    if (createErr) {
                        if ((createErr as zookeeper.Exception).getCode?.() === zookeeper.Exception.NODE_EXISTS) {
                            // Stale node from a previous session — update data in-place
                            this._zkClient!.setData(instancePath, data, -1, (setErr) => {
                                if (setErr) return reject(setErr);
                                resolve();
                            });
                        } else {
                            reject(createErr);
                        }
                    } else {
                        resolve();
                    }
                });
            });
        });
    }
}
