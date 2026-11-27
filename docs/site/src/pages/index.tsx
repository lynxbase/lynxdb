import type {ReactNode} from 'react';
import Head from '@docusaurus/Head';
import Link from '@docusaurus/Link';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import CodeBlock from '@theme/CodeBlock';
import Heading from '@theme/Heading';
import Layout from '@theme/Layout';

import MarkdownAlternate from '../components/MarkdownAlternate';
import content from '../../homepage.json';

import styles from './index.module.css';

const {title, description, lead, example, actions, guides, references} = content;
const [quickStart, install] = actions;

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
      description={description}>
      <MarkdownAlternate permalink={siteConfig.baseUrl} />
      <Head>
        <script type="application/ld+json">
          {JSON.stringify({
            '@context': 'https://schema.org',
            '@graph': [
              {
                '@type': 'WebSite',
                name: title,
                url: new URL(siteConfig.baseUrl, siteConfig.url).href,
                inLanguage: 'en',
                publisher: {'@id': 'https://lynxdb.org/#organization'},
                about: {'@id': 'https://lynxdb.org/#source-code'},
              },
              {
                '@type': 'Organization',
                '@id': 'https://lynxdb.org/#organization',
                name: 'LynxDB',
                url: 'https://lynxdb.org/',
              },
              {
                '@type': 'SoftwareSourceCode',
                '@id': 'https://lynxdb.org/#source-code',
                name: 'LynxDB',
                codeRepository: 'https://github.com/lynxbase/lynxdb',
                license: 'https://www.apache.org/licenses/LICENSE-2.0',
              },
            ],
          })}
        </script>
      </Head>
      <main>
        <header className={styles.hero}>
          <div className={`wrap ${styles.heroGrid}`}>
            <div className={styles.intro}>
              <Heading as="h1">{title}</Heading>
              <p>{lead}</p>
              <div className={styles.actions}>
                <Link className={styles.primary} to={quickStart.to}>
                  {quickStart.title} <Arrow />
                </Link>
                <Link className={styles.secondary} to={install.to}>{install.title}</Link>
              </div>
            </div>
            <figure className={styles.example}>
              <figcaption>{example.title}</figcaption>
              <div className={styles.command}>
                <CodeBlock language="bash">{example.command}</CodeBlock>
              </div>
              <div className={styles.result} aria-label="Example output">
                <pre>{example.output}</pre>
              </div>
            </figure>
          </div>
        </header>

        <div className={`wrap ${styles.directory}`}>
          <section aria-labelledby="guides-title">
            <Heading as="h2" id="guides-title">Guides</Heading>
            <ul>
              {guides.map(({title, description, to}) => (
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
