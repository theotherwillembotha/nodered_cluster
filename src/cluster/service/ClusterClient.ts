import { connect } from "@nats-io/transport-node";
import { NatsConnection, ConnectionOptions } from "@nats-io/nats-core";
import { jetstream, jetstreamManager } from "@nats-io/jetstream";
import { Kvm, KV } from "@nats-io/kv";
import { ClusterRole } from "../node/ClusterConfigNode";

export type ClusterClientParams = {
    instanceId:  string;
    zkRootPath:  string;
    natsAddress: string;
    natsUser?:   string;
    natsPass?:   string;
    role:        ClusterRole;
};

export type ClusterClientStatus = "idle" | "connecting" | "connected" | "disconnected" | "error";

type StatusListener = (status: ClusterClientStatus, error?: Error) => void;

export class ClusterClient {

    private _params:          ClusterClientParams;
    private _natsClient:      NatsConnection | null = null;
    private _kvBucket:        KV | null = null;
    private _status:          ClusterClientStatus = "idle";
    private _statusListeners: StatusListener[] = [];
    private _streamName:      string | null = null;

    constructor(params: ClusterClientParams) {
        this._params = params;
    }

    // ── Lifecycle ────────────────────────────────────────────────────────────

    public updateParams(params: ClusterClientParams): void {
        if (JSON.stringify(params) === JSON.stringify(this._params)) return;
        this._streamName = null;
        // stop() must run with the OLD params so it cleans up the correct KV keys,
        // then swap params in before start() so the new identity is registered.
        this.stop().then(() => {
            this._params = params;
            return this.start();
        }).catch(err => {
            console.error("ClusterClient: restart after param change failed:", err);
        });
    }

    public async start(): Promise<void> {
        this._setStatus("connecting");

        const connectOpts: ConnectionOptions = { servers: this._params.natsAddress };
        if (this._params.natsUser) connectOpts.user = this._params.natsUser;
        if (this._params.natsPass) connectOpts.pass = this._params.natsPass;
        this._natsClient = await connect(connectOpts);

        await this._initKV();

        this._setStatus("connected");
    }

    public async stop(): Promise<void> {
        if (this._kvBucket) {
            try {
                await this._kvBucket.delete(`instances.${this._params.instanceId}`);
            } catch (_) {}
            try {
                const iter = await this._kvBucket.keys(`endpoints.${this._params.instanceId}.>`);
                for await (const key of iter) {
                    try { await this._kvBucket.delete(key); } catch (_) {}
                }
            } catch (_) { /* no endpoint keys to clean up */ }
            this._kvBucket = null;
        }
        if (this._natsClient) {
            try { await this._natsClient.drain(); } catch (_) { /* ignore drain errors */ }
            this._natsClient = null;
        }
        this._streamName = null;
        this._setStatus("idle");
    }

    // ── Accessors ────────────────────────────────────────────────────────────

    public natsClient(): NatsConnection | null { return this._natsClient; }
    public kvBucket():   KV | null             { return this._kvBucket;   }
    public status():     ClusterClientStatus   { return this._status;     }
    public params():     ClusterClientParams   { return this._params;     }

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

    /** KV bucket name e.g. `CLUSTER_NODERED_CLUSTER` */
    public kvBucketName(): string {
        return "CLUSTER_" + this.rootPrefix().toUpperCase().replace(/[.\-\/]/g, "_");
    }

    // ── KV endpoint/instance helpers ─────────────────────────────────────────

    public async announceEndpoint(subject: string, mode: string, ttl: number): Promise<void> {
        const kv = this._kvBucket;
        if (!kv) return;
        await kv.put(
            `endpoints.${this._params.instanceId}.${subject}`,
            JSON.stringify({ mode, ttl })
        );
    }

    public async removeEndpoint(subject: string): Promise<void> {
        const kv = this._kvBucket;
        if (!kv) return;
        await kv.delete(`endpoints.${this._params.instanceId}.${subject}`);
    }

    // ── Stream management ────────────────────────────────────────────────────

    public async ensureStream(): Promise<string> {
        if (this._streamName) return this._streamName;

        const nc     = this._natsClient!;
        const jsm    = await jetstreamManager(nc);
        const prefix = this.rootPrefix();
        const name   = prefix.toUpperCase().replace(/[.\-\/]/g, "_");

        try {
            await jsm.streams.info(name);
        } catch (_) {
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

    /** Publish via core NATS (ephemeral  fire and forget). */
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

    private async _initKV(): Promise<void> {
        const kvm         = new Kvm(this._natsClient!);
        this._kvBucket    = await kvm.create(this.kvBucketName(), { history: 1 });

        await this._kvBucket.put(
            `instances.${this._params.instanceId}`,
            JSON.stringify({ natsAddress: this._params.natsAddress, role: this._params.role })
        );
    }
}
