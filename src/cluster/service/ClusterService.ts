import { BaseService, ServiceDescriptor } from "@theotherwillembotha/node-red-plugincore";
import { NodeAPI, NodeAPISettingsWithData } from "node-red";
import { connect } from "@nats-io/transport-node";
import * as zookeeper from "node-zookeeper-client";
import { ClusterClient, ClusterClientParams } from "./ClusterClient";

type TestResult = { ok: boolean; message?: string; error?: string };

export class ClusterService extends BaseService {

    private static _clients: { [configId: string]: ClusterClient } = {};
    private _red!: NodeAPI<NodeAPISettingsWithData>;

    constructor() {
        super("ClusterService");
    }

    // ── BaseService lifecycle ────────────────────────────────────────────────

    public async init(red: NodeAPI<NodeAPISettingsWithData>): Promise<void> {
        this._red = red;
        console.log("STARTING: ClusterService");

        const writePermission = red.auth.needsPermission("inject.write");

        red.httpAdmin.get("/cluster/status/:configId",  writePermission, (req, res) => this._handleStatus(req, res));
        red.httpAdmin.get("/cluster/members/:configId", writePermission, (req, res) => this._handleMembers(req, res));
        red.httpAdmin.post("/cluster/testconnection",   writePermission, (req, res) => this._handleTestConnection(req, res));
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
            // Config node redeployed — update params and restart if anything changed.
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
        const zk = client.zkClient();
        if (!zk || client.status() !== "connected") {
            res.status(503).json({ error: "Not connected", status: client.status() });
            return;
        }

        const zkRootPath = client.params().zkRootPath;
        zk.getChildren(`${zkRootPath}/instances`, (err, children) => {
            if (err) {
                res.status(500).json({ error: err.toString() });
                return;
            }
            res.json({ members: children ?? [] });
        });
    }

    // ── Test connection ──────────────────────────────────────────────────────

    private async _handleTestConnection(req: any, res: any): Promise<void> {
        const { natsAddress, zkAddress } = req.body as { natsAddress: string; zkAddress: string };
        const [nats, zk] = await Promise.all([
            this._testNats(natsAddress),
            this._testZK(zkAddress),
        ]);
        res.json({ nats, zk });
    }

    private async _testNats(address: string): Promise<TestResult> {
        try {
            const nc = await connect({ servers: address, maxReconnectAttempts: 0, timeout: 5000 });
            await nc.drain();
            return { ok: true, message: "Connected successfully" };
        } catch (err: any) {
            return { ok: false, error: err.message || "Connection failed" };
        }
    }

    private _testZK(address: string): Promise<TestResult> {
        return new Promise((resolve) => {
            const client = zookeeper.createClient(address);
            let done = false;

            const finish = (result: TestResult) => {
                if (done) return;
                done = true;
                clearTimeout(timeout);
                try { client.close(); } catch (_) {}
                resolve(result);
            };

            const timeout = setTimeout(() => {
                finish({ ok: false, error: "Connection timed out" });
            }, 5000);

            (client as any).addListener("error", (err: Error) => {
                finish({ ok: false, error: err.message || "Connection error" });
            });

            client.on("state", (state) => {
                if (["SYNC_CONNECTED", "CONNECTED_READ_ONLY"].includes(state.name)) {
                    finish({ ok: true, message: "Connected successfully" });
                } else if (state.name === "AUTH_FAILED") {
                    finish({ ok: false, error: "Authentication failed" });
                }
            });

            client.connect();
        });
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
