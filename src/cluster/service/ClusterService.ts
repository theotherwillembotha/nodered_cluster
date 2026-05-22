import { BaseService, ServiceDescriptor } from "@theotherwillembotha/node-red-plugincore";
import { NodeAPI, NodeAPISettingsWithData } from "node-red";
import { connect } from "@nats-io/transport-node";
import { ClusterClient, ClusterClientParams } from "./ClusterClient";

type TestResult = { ok: boolean; message?: string; error?: string };

export class ClusterService extends BaseService {

    private static _clients: { [configId: string]: ClusterClient } = {};

    constructor() {
        super("ClusterService");
    }

    // ── BaseService lifecycle ────────────────────────────────────────────────

    public async init(red: NodeAPI<NodeAPISettingsWithData>): Promise<void> {
        console.log("STARTING: ClusterService");

        const writePermission = red.auth.needsPermission("inject.write");

        red.httpAdmin.get("/cluster/status/:configId",    writePermission, (req, res) => this._handleStatus(req, res));
        red.httpAdmin.get("/cluster/members/:configId",   writePermission, (req, res) => this._handleMembers(req, res));
        red.httpAdmin.get("/cluster/discovery/:configId", writePermission, (req, res) => this._handleDiscovery(req, res));
        red.httpAdmin.post("/cluster/testconnection",     writePermission, (req, res) => this._handleTestConnection(req, res));
    }

    public deinit(_red: NodeAPI<NodeAPISettingsWithData>): void {
        console.log("STOPPING: ClusterService");
        Object.values(ClusterService._clients).forEach(client => {
            client.stop().catch(err => console.error("ClusterService: error stopping client:", err));
        });
        ClusterService._clients = {};
    }

    // ── Static client management ─────────────────────────────────────────────

    public static ensureClient(configId: string, params: ClusterClientParams): ClusterClient {
        let client = ClusterService._clients[configId];
        if (!client) {
            client = new ClusterClient(params);
            ClusterService._clients[configId] = client;
            client.start().catch(err => {
                console.error(`ClusterService: failed to start client for ${configId}:`, err);
            });
        } else {
            client.updateParams(params);
        }
        return client;
    }

    public static getClient(configId: string): ClusterClient | null {
        return ClusterService._clients[configId] ?? null;
    }

    public static removeClient(configId: string): void {
        const client = ClusterService._clients[configId];
        if (client) {
            client.stop().catch(err => console.error(`ClusterService: error stopping client ${configId}:`, err));
            delete ClusterService._clients[configId];
        }
    }

    // ── HTTP admin handlers ──────────────────────────────────────────────────

    private _handleStatus(req: any, res: any): void {
        const client = ClusterService._clients[req.params.configId];
        if (!client) {
            res.status(404).json({ error: "No client for that config node" });
            return;
        }
        res.json({ status: client.status(), params: client.params() });
    }

    private async _handleMembers(req: any, res: any): Promise<void> {
        const client = ClusterService._clients[req.params.configId];
        if (!client) {
            res.status(404).json({ error: "No client for that config node" });
            return;
        }
        const kv = client.kvBucket();
        if (!kv || client.status() !== "connected") {
            res.status(503).json({ error: "Not connected", status: client.status() });
            return;
        }

        try {
            const members: string[] = [];
            const iter = await kv.keys("instances.>");
            for await (const key of iter) {
                members.push(key.replace(/^instances\./, ""));
            }
            res.json({ members });
        } catch (err: any) {
            // No keys found is not an error  return empty list
            res.json({ members: [] });
        }
    }

    private async _handleDiscovery(req: any, res: any): Promise<void> {
        const client = ClusterService._clients[req.params.configId];
        if (!client) {
            res.status(404).json({ error: "No client for that config node" });
            return;
        }
        const kv = client.kvBucket();
        if (!kv || client.status() !== "connected") {
            res.status(503).json({ error: "Not connected", status: client.status() });
            return;
        }

        const instances: Record<string, any>      = {};
        const endpoints: Record<string, string[]> = {};

        try {
            const instanceIter = await kv.keys("instances.>");
            for await (const key of instanceIter) {
                const entry = await kv.get(key);
                if (entry && entry.operation === "PUT") {
                    const instanceId = key.replace(/^instances\./, "");
                    try { instances[instanceId] = JSON.parse(entry.string()); } catch (_) {}
                }
            }
        } catch (_) { /* no instance keys */ }

        try {
            const endpointIter = await kv.keys("endpoints.>");
            for await (const key of endpointIter) {
                const entry = await kv.get(key);
                if (entry && entry.operation === "PUT") {
                    // key: "endpoints.<instanceId>.<subject...>"
                    const withoutPrefix = key.replace(/^endpoints\./, "");
                    const dotIdx        = withoutPrefix.indexOf(".");
                    if (dotIdx === -1) continue;
                    const instanceId = withoutPrefix.substring(0, dotIdx);
                    const subject    = withoutPrefix.substring(dotIdx + 1);
                    if (!endpoints[instanceId]) endpoints[instanceId] = [];
                    endpoints[instanceId].push(subject);
                }
            }
        } catch (_) { /* no endpoint keys */ }

        res.json({ instances, endpoints });
    }

    // ── Test connection ──────────────────────────────────────────────────────

    private async _handleTestConnection(req: any, res: any): Promise<void> {
        const { natsAddress, natsUser, natsPass } = req.body as { natsAddress: string; natsUser?: string; natsPass?: string };
        const nats = await this._testNats(natsAddress, natsUser, natsPass);
        res.json({ nats });
    }

    private async _testNats(address: string, user?: string, pass?: string): Promise<TestResult> {
        try {
            const opts: any = { servers: address, maxReconnectAttempts: 0, timeout: 5000 };
            if (user) opts.user = user;
            if (pass) opts.pass = pass;
            const nc = await connect(opts);
            await nc.drain();
            return { ok: true, message: "Connected successfully" };
        } catch (err: any) {
            return { ok: false, error: err.message || "Connection failed" };
        }
    }

    // ── ServiceDescriptor ────────────────────────────────────────────────────

    static override getServiceDescriptor(): ServiceDescriptor {
        return new ServiceDescriptor(
            "@theotherwillembotha/clusterservice",
            "ClusterService",
            "integration-plugin",
            "./cluster/service/ClusterService",
            ClusterService
        );
    }
}
