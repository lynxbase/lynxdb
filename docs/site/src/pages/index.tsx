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
    description: 'Install LynxDB, query a local file, or start a server.',
    to: '/docs/getting-started/quickstart',
  },
  {
    title: 'PPL guide',
    description: 'Search events, extract fields, and aggregate logs. Look up supported commands and functions.',
    to: '/docs/ppl/overview',
  },
  {
    title: 'Ingest data',
    description: 'Send logs over HTTP, OpenTelemetry, Splunk HEC, or syslog. Find settings for your shipper.',
    to: '/docs/guides/ingest-data',
  },
  {
    title: 'Deploy and operate',
    description: 'Configure storage, retention, authentication, and backups for a single-node server.',
    to: '/docs/deployment/single-node',
  },
];

export default function Home(): ReactNode {
  const {siteConfig} = useDocusaurusContext();
  return (
    <Layout
      title="Log analytics database"
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
              <Heading as="h1">Log analytics<br /> in one binary</Heading>
              <p>Query files and standard input with OpenSearch PPL, or run a LynxDB server that stores logs on disk.</p>
              <div className={styles.actions}>
                <Link className="button button--primary button--lg" to="/docs/getting-started/quickstart">
                  Quick start
                </Link>
                <Link to="/docs/getting-started/install">Installation options</Link>
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

        <section className={styles.directory} aria-labelledby="docs-title">
          <Heading as="h2" id="docs-title">Documentation</Heading>
          <ul>
            {sections.map(({title, description, to}) => (
              <li key={to}>
                <Link to={to}>{title}</Link>
                <span>{description}</span>
              </li>
            ))}
          </ul>
          <nav className={styles.referenceLinks} aria-label="Reference">
            <Link to="/docs/cli/overview">CLI reference</Link>
            <Link to="/docs/api/overview">REST API</Link>
            <Link to="/docs/architecture/overview">Architecture</Link>
          </nav>
        </section>
      </main>
    </Layout>
  );
}
