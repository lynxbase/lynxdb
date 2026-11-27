import type {PrismTheme} from 'prism-react-renderer';
import type {Config} from '@docusaurus/types';
import type * as Preset from '@docusaurus/preset-classic';

const codeTheme: PrismTheme = {
  plain: {
    color: 'var(--lynx-code-text)',
    backgroundColor: 'var(--lynx-code-bg)',
  },
  styles: [
    {types: ['comment', 'prolog', 'doctype', 'cdata'], style: {color: 'var(--lynx-code-comment)'}},
    {types: ['keyword', 'tag', 'atrule', 'inserted'], style: {color: 'var(--lynx-code-keyword)'}},
    {types: ['function', 'property', 'attr-name', 'class-name'], style: {color: 'var(--lynx-code-function)'}},
    {types: ['string', 'char', 'attr-value', 'regex'], style: {color: 'var(--lynx-code-string)'}},
    {types: ['number', 'boolean', 'constant'], style: {color: 'var(--lynx-code-number)'}},
    {types: ['deleted', 'important'], style: {color: 'var(--lynx-code-error)'}},
  ],
};

const config: Config = {
  title: 'LynxDB',
  tagline: 'Log analytics in one binary',
  favicon: 'img/lynxdb-appicon-moss.svg',
  url: 'https://docs.lynxdb.org',
  baseUrl: '/',
  trailingSlash: true,
  organizationName: 'lynxbase',
  projectName: 'lynxdb',
  onBrokenLinks: 'throw',

  headTags: [
    {
      tagName: 'link',
      attributes: {
        rel: 'preload',
        href: '/fonts/geist-latin-wght-normal.woff2',
        as: 'font',
        type: 'font/woff2',
        crossorigin: 'anonymous',
      },
    },
  ],

  future: {
    v4: true,
  },

  i18n: {
    defaultLocale: 'en',
    locales: ['en'],
  },

  markdown: {
    mermaid: true,
  },

  themes: ['@docusaurus/theme-mermaid'],

  presets: [
    [
      'classic',
      {
        docs: {
          sidebarPath: './sidebars.ts',
          editUrl: 'https://github.com/lynxbase/lynxdb/edit/main/docs/site/',
        },
        blog: {
          blogTitle: 'LynxDB Blog',
          blogDescription: 'LynxDB releases, storage and query engine updates, and log analytics benchmarks.',
          showReadingTime: true,
          editUrl: 'https://github.com/lynxbase/lynxdb/edit/main/docs/site/',
          onInlineTags: 'warn',
          onInlineAuthors: 'warn',
          onUntruncatedBlogPosts: 'warn',
        },
        theme: {
          customCss: './src/css/custom.css',
        },
        sitemap: {
          lastmod: 'date',
          changefreq: null,
          priority: null,
          ignorePatterns: [
            '/404',
            '/search/',
            '/blog/archive/',
            '/blog/authors/**',
            '/blog/tags/**',
            '/docs/tags/**',
          ],
        },
      } satisfies Preset.Options,
    ],
  ],

  plugins: [
    './plugins/crawler-content.mjs',
    [
      '@easyops-cn/docusaurus-search-local',
      {
        hashed: true,
        language: ['en'],
        highlightSearchTermsOnTargetPage: true,
        explicitSearchResultPath: true,
      },
    ],
  ],

  themeConfig: {
    image: 'img/lynxdb-share.jpg',
    metadata: [{property: 'og:site_name', content: 'LynxDB'}],
    colorMode: {
      defaultMode: 'light',
      respectPrefersColorScheme: true,
    },
    navbar: {
      title: 'LynxDB',
      logo: {
        alt: '',
        src: 'img/lynxdb-mark-faceted-on-light.svg',
        srcDark: 'img/lynxdb-mark-faceted-on-dark.svg',
        width: 22,
        height: 30,
      },
      items: [
        {
          type: 'docSidebar',
          sidebarId: 'docs',
          label: 'Docs',
          position: 'left',
        },
        {
          to: '/docs/api/overview',
          label: 'API',
          position: 'left',
        },
        {to: '/blog', label: 'Blog', position: 'left'},
        {
          href: 'https://github.com/lynxbase/lynxdb',
          label: 'GitHub',
          position: 'right',
        },
        {
          href: 'https://discord.gg/RgggCFdgWK',
          label: 'Discord',
          position: 'right',
        },
      ],
    },
    footer: {
      style: 'light',
      links: [
        {
          title: 'Docs',
          items: [
            {
              label: 'Quick start',
              to: '/docs/getting-started/quickstart',
            },
            {
              label: 'PPL guide',
              to: '/docs/ppl/overview',
            },
            {
              label: 'REST API',
              to: '/docs/api/overview',
            },
            {
              label: 'CLI reference',
              to: '/docs/cli/overview',
            },
          ],
        },
        {
          title: 'Community',
          items: [
            {
              label: 'Discord',
              href: 'https://discord.gg/RgggCFdgWK',
            },
            {
              label: 'GitHub',
              href: 'https://github.com/lynxbase/lynxdb',
            },
            {
              label: 'X (@o3vgeny)',
              href: 'https://x.com/o3vgeny',
            },
          ],
        },
        {
          title: 'More',
          items: [
            {
              label: 'LynxDB website',
              href: 'https://lynxdb.org/',
            },
            {
              label: 'Blog',
              to: '/blog',
            },
            {
              label: 'Releases',
              href: 'https://github.com/lynxbase/lynxdb/releases',
            },
          ],
        },
      ],
      copyright: `Copyright ${new Date().getFullYear()} LynxDB Authors. Apache 2.0 License.`,
    },
    docs: {
      sidebar: {
        hideable: true,
        autoCollapseCategories: true,
      },
    },
    prism: {
      theme: codeTheme,
      darkTheme: codeTheme,
      additionalLanguages: ['bash', 'yaml', 'json', 'go', 'sql'],
    },
  } satisfies Preset.ThemeConfig,
};

export default config;
