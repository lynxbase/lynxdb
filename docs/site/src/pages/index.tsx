import type {ReactNode} from 'react';
import clsx from 'clsx';
import Link from '@docusaurus/Link';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import Layout from '@theme/Layout';
import Heading from '@theme/Heading';

import styles from './index.module.css';

function TerminalMockup() {
  return (
    <div className="terminal">
      <div className="terminal-header">
        <span className="terminal-dot terminal-dot--red" />
        <span className="terminal-dot terminal-dot--yellow" />
        <span className="terminal-dot terminal-dot--green" />
      </div>
      <div className="terminal-body">
        <div><span className="prompt">$</span> cat app.log | lynxdb query '| stats count by level'</div>
        <br />
        <div className="output">  LEVEL    COUNT</div>
        <div className="output">  ─────────────────</div>
        <div className="output">  INFO     42,847</div>
        <div className="output">  ERROR     3,291</div>
        <div className="output">  WARN      1,104</div>
        <br />
        <div className="success">  ✔ 12ms — scanned 47,242 events</div>
      </div>
    </div>
  );
}

function HomepageHeader() {
  const {siteConfig} = useDocusaurusContext();
  return (
    <header className={clsx('hero hero--lynxdb', styles.heroBanner)}>
      <div className="container">
        <Heading as="h1" className="hero__title">
          {siteConfig.title}
        </Heading>
        <p className="hero__subtitle">{siteConfig.tagline}</p>
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
  icon: string;
  description: ReactNode;
};

const pillars: FeatureItem[] = [
  {
    title: 'One Binary, Every Scale',
    icon: '📦',
    description: (
      <>
        From a developer's laptop to a 1000-node cluster. Same binary, same query
        language, same API. No JVM, no Elasticsearch, no Kafka, no Zookeeper.
      </>
    ),
  },
  {
    title: 'SPL2 Query Language',
    icon: '🔍',
    description: (
      <>
        Splunk-inspired pipeline query language with 25+ commands, 15+ aggregation
        functions, CTEs, joins, and materialized views. One language everywhere.
      </>
    ),
  },
  {
    title: 'Pipe Mode to Cluster',
    icon: '⚡',
    description: (
      <>
        Query local files and stdin without a server. Start a single-node server.
        Scale to a distributed cluster. The binary adapts to your needs.
      </>
    ),
  },
];

const differentiators: FeatureItem[] = [
  {
    title: 'Zero Dependencies',
    icon: '🎯',
    description: (
      <>
        Static binary, zero runtime dependencies. Install with a single <code>curl</code>.
        No JVM, no package managers, no shared libraries.
      </>
    ),
  },
  {
    title: 'Schema-on-Read',
    icon: '📝',
    description: (
      <>
        No upfront schema definition. Send any JSON, any text, any format.
        Fields are discovered and indexed automatically.
      </>
    ),
  },
  {
    title: '~50 MB Memory',
    icon: '💾',
    description: (
      <>
        Idle memory footprint of ~50 MB vs ~12 GB for Splunk. Runs on a $5/month
        VPS or a Raspberry Pi.
      </>
    ),
  },
];

function FeatureCards({features}: {features: FeatureItem[]}) {
  return (
    <div className="row">
      {features.map(({title, icon, description}, idx) => (
        <div key={idx} className={clsx('col col--4')} style={{marginBottom: '1.5rem'}}>
          <div className="feature-card">
            <div style={{fontSize: '2rem', marginBottom: '0.5rem'}}>{icon}</div>
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
            <td><strong>Query language</strong></td>
            <td><strong>SPL2</strong></td>
            <td>SPL</td>
            <td>Lucene DSL / ES|QL</td>
            <td>LogQL</td>
            <td>SQL</td>
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
            <td><strong>Memory (idle)</strong></td>
            <td><strong>~50 MB</strong></td>
            <td>~12 GB</td>
            <td>~1 GB+</td>
            <td>~256 MB</td>
            <td>~1 GB</td>
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
      description="LynxDB is an open-source log analytics database. Single binary, zero dependencies, SPL2 query language. From pipe mode to 1000-node clusters.">
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
