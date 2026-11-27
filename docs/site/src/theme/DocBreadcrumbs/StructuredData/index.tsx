import type {ReactNode} from 'react';
import Head from '@docusaurus/Head';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import type {Props} from '@theme/DocBreadcrumbs/StructuredData';

export default function StructuredData({breadcrumbs}: Props): ReactNode {
  const {siteConfig} = useDocusaurusContext();
  const home = new URL(siteConfig.baseUrl, siteConfig.url).href;
  const trail = new Map([[home, siteConfig.title]]);
  for (const crumb of breadcrumbs) {
    if (crumb.href && !(crumb.type === 'category' && crumb.linkUnlisted)) {
      const url = new URL(crumb.href, home);
      url.pathname = url.pathname.replace(/\/?$/, '/');
      trail.set(url.href, crumb.label);
    }
  }
  if (trail.size < 2) {
    return null;
  }
  return (
    <Head>
      <script type="application/ld+json">
        {JSON.stringify({
          '@context': 'https://schema.org',
          '@type': 'BreadcrumbList',
          itemListElement: Array.from(trail, ([item, name], index) => ({
            '@type': 'ListItem',
            position: index + 1,
            name,
            item,
          })),
        }).replace(/</g, '\\u003c')}
      </script>
    </Head>
  );
}
