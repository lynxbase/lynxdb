module.exports = ({mode}: {mode?: string}) => {
  const preview = mode === 'preview';
  return {
    worker: {
      name: preview ? 'lynxdb-docs-preview' : 'lynxdb-docs',
      compatibilityDate: '2026-11-27',
      entrypoint: './worker/index.mjs',
      workersDev: preview,
      previewUrls: false,
      assets: {
        htmlHandling: 'auto-trailing-slash',
        notFoundHandling: '404-page',
        runWorkerFirst: ['/', '/docs/*', '!/docs/*.md'],
      },
      env: {ASSETS: {type: 'assets'}},
      observability: {
        enabled: true,
        traces: {enabled: true},
      },
      triggers: preview ? [] : [
        {type: 'fetch', pattern: 'docs.lynxdb.org/*', zone: 'lynxdb.org'},
      ],
    },
  };
};
