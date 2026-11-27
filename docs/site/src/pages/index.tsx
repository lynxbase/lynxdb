import type {ReactNode} from 'react';
import Head from '@docusaurus/Head';
import Link from '@docusaurus/Link';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import CodeBlock from '@theme/CodeBlock';
import Heading from '@theme/Heading';
import Layout from '@theme/Layout';

import styles from './index.module.css';

const query = String.raw`lynxdb query --file access.ndjson \
  --format csv --no-stats '
  source=main
  | where status >= 500
  | stats count() as errors by service'`;

const sections = [
  {
    title: 'Getting started',
    description: 'Install LynxDB and run your first query on a local file or a server.',
    to: '/docs/getting-started/quickstart',
  },
  {
    title: 'PPL guide',
    description: 'Search events, extract fields, and look up PPL commands and functions.',
    to: '/docs/ppl/overview',
  },
  {
    title: 'Ingest data',
    description: 'Send logs over HTTP, OpenTelemetry, Splunk HEC, or syslog.',
    to: '/docs/guides/ingest-data',
  },
  {
    title: 'Deploy and operate',
    description: 'Configure storage, retention, authentication, and backups.',
    to: '/docs/deployment/single-node',
  },
];

const references = [
  {title: 'CLI reference', to: '/docs/cli/overview'},
  {title: 'REST API', to: '/docs/api/overview'},
  {title: 'Architecture', to: '/docs/architecture/overview'},
];

function Arrow(): ReactNode {
  return (
    <svg aria-hidden="true" viewBox="0 0 16 16" width="16" height="16" fill="none">
      <path d="M3 8h9m-4-4 4 4-4 4" stroke="currentColor" strokeWidth="1.4" strokeLinecap="round" strokeLinejoin="round" />
    </svg>
  );
}

export default function Home(): ReactNode {
  const {siteConfig} = useDocusaurusContext();
  return (
    <Layout
      title="Documentation"
      description="Query log files and standard input with OpenSearch PPL, or run a LynxDB server that stores logs on disk.">
      <Head>
        <script type="application/ld+json">
          {JSON.stringify({
            '@context': 'https://schema.org',
            '@type': 'WebSite',
            name: 'LynxDB',
            url: new URL(siteConfig.baseUrl, siteConfig.url).href,
            inLanguage: 'en',
          })}
        </script>
      </Head>
      <main>
        <header className={styles.hero}>
          <div className={styles.heroGrid}>
            <div className={styles.intro}>
              <Heading as="h1">LynxDB documentation</Heading>
              <p>Query log files and standard input with OpenSearch PPL, or run a server to ingest and search logs from your services.</p>
              <div className={styles.actions}>
                <Link className={styles.primary} to="/docs/getting-started/quickstart">
                  Quick start <Arrow />
                </Link>
                <Link className={styles.secondary} to="/docs/getting-started/install">Install LynxDB</Link>
              </div>
            </div>
            <figure className={styles.example}>
              <figcaption>Count HTTP 5xx responses by service</figcaption>
              <div className={styles.command}>
                <CodeBlock language="bash">{query}</CodeBlock>
              </div>
              <div className={styles.result} aria-label="Example output">
                <pre>{'errors,service\n2,api'}</pre>
              </div>
            </figure>
          </div>
        </header>

        <div className={styles.directory}>
          <section aria-labelledby="guides-title">
            <Heading as="h2" id="guides-title">Guides</Heading>
            <ul>
              {sections.map(({title, description, to}) => (
                <li key={to}>
                  <Link className={styles.topic} to={to}>
                    <span className={styles.topicTitle}>{title}</span>
                    <span className={styles.topicDescription}>{description}</span>
                    <Arrow />
                  </Link>
                </li>
              ))}
            </ul>
          </section>
          <section aria-labelledby="reference-title">
            <Heading as="h2" id="reference-title">Reference</Heading>
            <ul>
              {references.map(({title, to}) => (
                <li key={to}>
                  <Link className={styles.topic} to={to}>
                    <span className={styles.topicTitle}>{title}</span>
                    <Arrow />
                  </Link>
                </li>
              ))}
            </ul>
          </section>
        </div>
      </main>
    </Layout>
  );
}
