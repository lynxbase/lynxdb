import type {SidebarsConfig} from '@docusaurus/plugin-content-docs';

const sidebars: SidebarsConfig = {
  docs: [
    'intro',
    {
      type: 'category',
      label: 'Getting started',
      collapsed: false,
      items: [
        'getting-started/quickstart',
        'getting-started/install',
        'getting-started/pipe-mode',
        'getting-started/server-mode',
        'getting-started/first-query',
      ],
    },
    {
      type: 'category',
      label: 'PPL',
      link: {type: 'doc', id: 'ppl/overview'},
      items: [
        'ppl/getting-started',
        {
          type: 'category',
          label: 'Language',
          items: [
            'ppl/syntax',
            'ppl/grammar',
            'ppl/data-types',
            'ppl/expressions',
            'ppl/search-and-types',
            'ppl/time-series',
            'ppl/joins-and-subsearches',
          ],
        },
        {
          type: 'category',
          label: 'Reference',
          items: [
            'ppl/command-guide',
            'ppl/commands',
            'ppl/function-guide',
            'ppl/functions',
            'ppl/aggregation-guide',
            'ppl/aggregations',
            'ppl/feature-matrix',
            'ppl/errors',
          ],
        },
        {
          type: 'category',
          label: 'Using PPL',
          items: ['ppl/examples', 'ppl/pipeline-execution', 'ppl/performance'],
        },
      ],
    },
    {
      type: 'category',
      label: 'Guides',
      items: [
        {
          type: 'category',
          label: 'Ingest data',
          link: {type: 'doc', id: 'guides/ingest-data'},
          items: [
            'guides/ingest-data/filebeat',
            'guides/ingest-data/fluent-bit',
            'guides/ingest-data/vector',
            'guides/ingest-data/opentelemetry',
            'guides/ingest-data/splunk-hec',
          ],
        },
        'guides/field-extraction',
        'guides/json-processing',
        'guides/kubernetes-audit-logs',
        'guides/scripting-and-pipes',
      ],
    },
    {
      type: 'category',
      label: 'CLI reference',
      items: [
        'cli/overview',
        'cli/query',
        'cli/explain',
        'cli/ingest',
        'cli/server',
        'cli/status',
        'cli/auth',
        'cli/config-cmd',
        'cli/output-formats',
        'cli/completion',
      ],
    },
    {
      type: 'category',
      label: 'REST API',
      items: [
        'api/overview',
        'api/ingest',
        'api/query',
        'api/histogram',
        'api/fields',
        'api/server',
        'api/compatibility',
      ],
    },
    {
      type: 'category',
      label: 'Configuration',
      items: [
        'configuration/overview',
        'configuration/server',
        'configuration/storage',
        'configuration/query',
        'configuration/ingest',
        'configuration/syslog',
        'configuration/environment-variables',
      ],
    },
    {
      type: 'category',
      label: 'Deployment',
      items: [
        'deployment/single-node',
        'deployment/docker',
        'deployment/kubernetes',
        'deployment/tls-auth',
      ],
    },
    {
      type: 'category',
      label: 'Operations',
      items: [
        'operations/monitoring',
        'operations/backup-restore',
        'operations/retention',
        'operations/performance-tuning',
        'operations/shipper-conformance',
        'operations/troubleshooting',
        'operations/upgrading',
      ],
    },
    {
      type: 'category',
      label: 'Architecture',
      items: [
        'architecture/overview',
        'architecture/storage',
        'architecture/indexing',
        'architecture/query-engine',
        'architecture/es-stubs',
        'architecture/design-decisions',
      ],
    },
    {
      type: 'category',
      label: 'Contributing',
      items: [
        'contributing/development-setup',
        'contributing/project-structure',
        'contributing/coding-guidelines',
        {
          type: 'category',
          label: 'Query language internals',
          items: [
            'contributing/ppl-architecture',
            'contributing/ppl-compilation',
            'contributing/ir',
            'contributing/ir-optimizer',
            'contributing/adding-commands',
            'contributing/adding-functions',
          ],
        },
      ],
    },
  ],
};

export default sidebars;
