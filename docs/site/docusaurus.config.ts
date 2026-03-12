import {themes as prismThemes} from 'prism-react-renderer';
import type {Config} from '@docusaurus/types';
import type * as Preset from '@docusaurus/preset-classic';

const config: Config = {
  title: 'LynxDB',
  tagline: 'Log analytics in a single binary. Zero dependencies. Lynx Flow query language.',
  favicon: 'img/favicon.ico',
  url: 'https://docs.lynxdb.org',
  baseUrl: '/',
  organizationName: 'lynxdb',
  projectName: 'lynxdb',
  onBrokenLinks: 'throw',

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
          // Enable once docs are committed to git:
          // showLastUpdateTime: true,
          // showLastUpdateAuthor: true,
        },
        blog: {
          showReadingTime: true,
          editUrl: 'https://github.com/lynxbase/lynxdb/edit/main/docs/site/',
          onInlineTags: 'warn',
          onInlineAuthors: 'warn',
          onUntruncatedBlogPosts: 'warn',
        },
        theme: {
          customCss: './src/css/custom.css',
        },
      } satisfies Preset.Options,
    ],
  ],

  plugins: [
    [
      require.resolve('@easyops-cn/docusaurus-search-local'),
      {
        hashed: true,
        language: ['en'],
        highlightSearchTermsOnTargetPage: true,
        explicitSearchResultPath: true,
      },
    ],
  ],

  themeConfig: {
    image: 'img/og-image.png',
    colorMode: {
      defaultMode: 'light',
      respectPrefersColorScheme: true,
    },
    announcementBar: {
      id: 'star-us',
      content:
        'If you like LynxDB, give us a <a href="https://github.com/lynxbase/lynxdb">star on GitHub</a>!',
      isCloseable: true,
    },
    navbar: {
      title: 'LynxDB',
      logo: {
        alt: 'LynxDB Logo',
        src: 'img/lynxdb-icon.png',
        srcDark: 'img/lynxdb-icon.png',
        style: {height: '32px'},
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
      style: 'dark',
      links: [
        {
          title: 'Docs',
          items: [
            {
              label: 'Quick Start',
              to: '/docs/getting-started/quickstart',
            },
            {
              label: 'Lynx Flow Reference',
              to: '/docs/lynx-flow/overview',
            },
            {
              label: 'REST API',
              to: '/docs/api/overview',
            },
            {
              label: 'CLI Reference',
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
              label: 'Twitter',
              href: 'https://twitter.com/lynxdb',
            },
          ],
        },
        {
          title: 'More',
          items: [
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
      theme: prismThemes.github,
      darkTheme: prismThemes.dracula,
      additionalLanguages: ['bash', 'yaml', 'json', 'go', 'sql'],
    },
  } satisfies Preset.ThemeConfig,
};

export default config;
