import type {ReactNode} from 'react';
import clsx from 'clsx';
import Link from '@docusaurus/Link';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import Layout from '@theme/Layout';
import Heading from '@theme/Heading';

import styles from './index.module.css';

const subtitle =
  'Log analytics in a single binary. Zero dependencies. Queried with OpenSearch PPL.';

function TerminalMockup() {
  return (
    <div className="terminal">
      <div className="terminal-header">
        <span className="terminal-dot terminal-dot--red" />
        <span className="terminal-dot terminal-dot--yellow" />
        <span className="terminal-dot terminal-dot--green" />
      </div>
      <div className="terminal-body">
        <div>
          <span className="prompt">$</span> cat access.log | lynxdb query 'source=main | stats count() as n by level | sort - n'
        </div>
        <br />
        <div className="output">┌───────┬─────┐</div>
        <div className="output">│ level │ n   │</div>
        <div className="output">├───────┼─────┤</div>
        <div className="output">│ INFO  │ 359 │</div>
        <div className="output">│ WARN  │ 347 │</div>
        <div className="output">│ ERROR │ 294 │</div>
        <div className="output">└───────┴─────┘</div>
        <br />
        <div className="success">  (3 rows)</div>
      </div>
    </div>
  );
}

function HomepageHeader() {
  const {siteConfig} = useDocusaurusContext();
  return (
    <header className={clsx('hero hero--lynxdb', styles.heroBanner)}>
      <div className="container">
        <img
          src="/img/lynxdb-icon.png"
          alt="LynxDB Logo"
          className={styles.heroLogo}
        />
        <Heading as="h1" className="hero__title">
          {siteConfig.title}
        </Heading>
        <p className="hero__subtitle">{subtitle}</p>
        <div className={styles.buttons}>
          <Link
            className="button button--secondary button--lg"
            to="/docs/getting-started/quickstart">
            Get Started
          </Link>
          <Link
            className="button button--outline button--lg"
            style={{color: 'white', borderColor: 'rgba(255,255,255,0.4)', marginLeft: '1rem'}}
            href="https://github.com/lynxbase/lynxdb">
            GitHub
          </Link>
        </div>
        <TerminalMockup />
      </div>
    </header>
  );
}

type FeatureItem = {
  title: string;
  description: ReactNode;
};

const pillars: FeatureItem[] = [
  {
    title: 'One Binary',
    description: (
      <>
        Query files from the command line, or run a server with on-disk
        storage and a REST API. One statically linked binary, no JVM and no
        external services.
      </>
    ),
  },
  {
    title: 'OpenSearch PPL',
    description: (
      <>
        Query with OpenSearch Piped Processing Language: search terms,{' '}
        <code>where</code>, <code>stats</code>, <code>timechart</code>,{' '}
        <code>parse</code>, <code>rex</code>, <code>grok</code>,{' '}
        <code>spath</code>, joins and subsearches. Each query compiles to one
        plan that runs the same way on a file and on a server.
      </>
    ),
  },
  {
    title: 'Pipe Mode',
    description: (
      <>
        Pipe <code>kubectl logs</code>, <code>docker logs</code> or any file
        into <code>lynxdb query</code>. No daemon and no config: load into a
        temporary directory, query, print, exit.
      </>
    ),
  },
];

const differentiators: FeatureItem[] = [
  {
    title: 'Schema-on-Read',
    description: (
      <>
        No schema to declare. JSON keys and <code>key=value</code> pairs become
        typed fields on ingest; <code>parse</code>, <code>grok</code> and{' '}
        <code>spath</code> extract the rest at query time.
      </>
    ),
  },
  {
    title: 'Columnar Storage',
    description: (
      <>
        Every index is a columnar store, partitioned by time. Each block of
        events carries token filters, so searches skip the blocks that cannot
        match and scan the rest on every core.
      </>
    ),
  },
  {
    title: 'Drop-in Ingest',
    description: (
      <>
        Point Filebeat, Fluent Bit, Vector, the OpenTelemetry Collector,
        Splunk HEC senders or syslog at LynxDB without changing their
        protocols.
      </>
    ),
  },
];

function FeatureCards({features}: {features: FeatureItem[]}) {
  return (
    <div className="row">
      {features.map(({title, description}, idx) => (
        <div key={idx} className={clsx('col col--4')} style={{marginBottom: '1.5rem'}}>
          <div className="feature-card">
            <Heading as="h3">{title}</Heading>
            <p>{description}</p>
          </div>
        </div>
      ))}
    </div>
  );
}

function ComparisonTable() {
  return (
    <div className="comparison-table">
      <table>
        <thead>
          <tr>
            <th></th>
            <th>LynxDB</th>
            <th>Splunk</th>
            <th>Elasticsearch</th>
            <th>Loki</th>
            <th>ClickHouse</th>
          </tr>
        </thead>
        <tbody>
          <tr>
            <td><strong>Deployment</strong></td>
            <td>Single binary</td>
            <td>Standalone or distributed</td>
            <td>Single node or cluster</td>
            <td>Single binary or microservices</td>
            <td>Single binary or cluster</td>
          </tr>
          <tr>
            <td><strong>Dependencies</strong></td>
            <td><strong>None</strong></td>
            <td>—</td>
            <td>Bundled JVM</td>
            <td>Object storage (prod)</td>
            <td>Keeper (replication)</td>
          </tr>
          <tr>
            <td><strong>Pipe mode</strong></td>
            <td><strong>Yes</strong></td>
            <td>No</td>
            <td>No</td>
            <td>No</td>
            <td>Yes</td>
          </tr>
          <tr>
            <td><strong>Schema</strong></td>
            <td><strong>On-read</strong></td>
            <td>On-read</td>
            <td>On-write</td>
            <td>Labels + line</td>
            <td>On-write</td>
          </tr>
          <tr>
            <td><strong>License</strong></td>
            <td><strong>Apache 2.0</strong></td>
            <td>Commercial</td>
            <td>ELv2 / AGPL</td>
            <td>AGPL</td>
            <td>Apache 2.0</td>
          </tr>
        </tbody>
      </table>
    </div>
  );
}

function InstallCTA() {
  return (
    <div style={{textAlign: 'center', padding: '3rem 0'}}>
      <Heading as="h2">Get Started in Seconds</Heading>
      <div className="install-block">
        <code>curl -fsSL https://lynxdb.org/install.sh | sh</code>
      </div>
      <div style={{marginTop: '1.5rem'}}>
        <Link
          className="button button--primary button--lg"
          to="/docs/getting-started/quickstart">
          Read the Quick Start Guide
        </Link>
      </div>
    </div>
  );
}

export default function Home(): ReactNode {
  return (
    <Layout
      title="Log Analytics in a Single Binary"
      description="LynxDB is an open-source log analytics database in a single binary with zero dependencies, queried with OpenSearch PPL on local files or a server.">
      <HomepageHeader />
      <main>
        <section style={{padding: '3rem 0'}}>
          <div className="container">
            <FeatureCards features={pillars} />
          </div>
        </section>
        <section style={{padding: '2rem 0', background: 'var(--ifm-color-emphasis-100)'}}>
          <div className="container">
            <Heading as="h2" style={{textAlign: 'center', marginBottom: '2rem'}}>
              Key Differentiators
            </Heading>
            <FeatureCards features={differentiators} />
          </div>
        </section>
        <section style={{padding: '3rem 0'}}>
          <div className="container">
            <Heading as="h2" style={{textAlign: 'center', marginBottom: '1rem'}}>
              How LynxDB Compares
            </Heading>
            <ComparisonTable />
          </div>
        </section>
        <InstallCTA />
      </main>
    </Layout>
  );
}
