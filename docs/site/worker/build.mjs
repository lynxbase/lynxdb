import {access, copyFile, mkdir} from 'node:fs/promises';
import path from 'node:path';
import {
  cleanBuildOutputDir, getWorkerBundleDir, writeAssets, writeRootConfig, writeWorkerConfig,
} from '@cloudflare/build-output-utils';
import config from '../cloudflare.config.ts';

const root = path.resolve(import.meta.dirname, '..');
const sourceDirectory = path.join(root, 'build');
const mode = process.argv[2];
const buildContext = {isPreview: false, mode};
const {worker} = config(buildContext);
const mainModule = path.basename(worker.entrypoint);

await access(path.join(sourceDirectory, 'index.md'));
await cleanBuildOutputDir(root);
const bundleDirectory = getWorkerBundleDir(root);
await mkdir(bundleDirectory, {recursive: true});
await Promise.all([
  copyFile(path.join(root, worker.entrypoint), path.join(bundleDirectory, mainModule)),
  writeAssets({root, sourceDirectory}),
  writeRootConfig({root, buildContext}),
  writeWorkerConfig({
    root,
    config: worker,
    manifest: {type: 'complete', mainModule, modules: {[mainModule]: {type: 'esm'}}},
  }),
]);
