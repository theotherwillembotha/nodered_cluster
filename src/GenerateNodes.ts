import { NodeGenerator } from "@theotherwillembotha/node-red-plugincore"

// services.
import { ClusterService } from "./cluster/service/ClusterService";

// nodes.
import { ClusterConfigNode } from "./cluster/node/ClusterConfigNode";
import { ClusterPublishNode } from "./cluster/node/ClusterPublishNode";
import { ClusterSubscribeNode } from "./cluster/node/ClusterSubscribeNode";

new NodeGenerator("./src/")
    // services.
    .registerService(ClusterService)

    // nodes
    .registerNode(ClusterConfigNode)
    .registerNode(ClusterPublishNode)
    .registerNode(ClusterSubscribeNode)

    // done.
    .generate("./build/Nodes", "./build/Plugins");

process.exit(0);
