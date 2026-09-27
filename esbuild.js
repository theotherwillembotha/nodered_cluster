const esbuild = require('esbuild');
const path = require('path');

// Only build-time deps and the Node-RED runtime are external.
// plugincore is NOT external - it is bundled inline for self-contained deployment.
const external = [
    'node-red',
    // plugincore build-time deps - lazy require()s, only needed during node generation
    'jsdom', 'js-beautify', 'markdown-it',
];

const sharedConfig = {
    bundle: true,
    platform: 'node',
    target: 'node18',
    external,
    format: 'cjs',
    alias: {
        // Nodes.js self-references the package - alias to local build output
        '@theotherwillembotha/node-red-cluster': path.resolve('./build/index.js'),
    },
};

async function build() {
    await esbuild.build({ ...sharedConfig, entryPoints: ['build/Nodes.js'],   outfile: 'build/Nodes.js',   allowOverwrite: true });
    console.log('Bundled Nodes.js');
    await esbuild.build({ ...sharedConfig, entryPoints: ['build/Plugins.js'], outfile: 'build/Plugins.js', allowOverwrite: true });
    console.log('Bundled Plugins.js');
}

build().catch(err => { console.error(err); process.exit(1); });
