import type {ReactNode} from 'react';
import Head from '@docusaurus/Head';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';

export default function MarkdownAlternate({permalink, unlisted}: {
  permalink: string;
  unlisted?: boolean;
}): ReactNode {
  const {siteConfig} = useDocusaurusContext();
  if (unlisted || siteConfig.noIndex) {
    return null;
  }
  const markdownPath = permalink === siteConfig.baseUrl
    ? `${siteConfig.baseUrl}index.md`
    : `${permalink.replace(/\/$/, '')}.md`;
  const url = new URL(markdownPath, siteConfig.url);
  return <Head><link rel="alternate" type="text/markdown" href={url.href} /></Head>;
}
