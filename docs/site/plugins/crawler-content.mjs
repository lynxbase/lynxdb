import {mkdir, readFile, writeFile} from 'node:fs/promises';
import path from 'node:path';
import {
  aliasedSitePathToRelativePath,
  escapeMarkdownHeadingIds,
  flattenRoutes,
  parseMarkdownFile,
  parseMarkdownHeadingId,
} from '@docusaurus/utils';
import {applyTrailingSlash} from '@docusaurus/utils-common';
import comment from '@slorber/remark-comment';
import yaml from 'js-yaml';
import directive from 'remark-directive';
import gfm from 'remark-gfm';
import mdx from 'remark-mdx';
import parse from 'remark-parse';
import stringify from 'remark-stringify';
import {unified} from 'unified';
import {visit} from 'unist-util-visit';

const markdown = unified()
  .use(parse)
  .use(gfm, {tableCellPadding: false, tablePipeAlign: false})
  .use(directive)
  .use(mdx)
  .use(comment)
  .use(stringify, {bullet: '-', fences: true});

const paragraph = (value) => ({type: 'paragraph', children: [{type: 'text', value}]});

function plainBlocks(nodes) {
  return nodes.flatMap((node) => {
    switch (node.type) {
      case 'mdxjsEsm':
        return [];
      case 'mdxJsxFlowElement': {
        const blocks = plainBlocks(node.children);
        if (node.name === 'TabItem') {
          const label = node.attributes.find(({name}) => name === 'label')?.value;
          return [{type: 'heading', depth: 3, children: [{type: 'text', value: label}]}, ...blocks];
        }
        if (node.name === 'Tabs') {
          return blocks;
        }
        break;
      }
      case 'containerDirective': {
        const title = node.name[0].toUpperCase() + node.name.slice(1);
        return [{type: 'blockquote', children: [paragraph(title), ...plainBlocks(node.children)]}];
      }
      default:
        return node;
    }
    throw new Error(`Missing Markdown rendering for <${node.name}>`);
  });
}

function canonicalLink(value, page, bySource, byRoute) {
  value = value.replace(/^pathname:\/\//, '');
  const url = new URL(value, page.url);
  if (url.origin !== page.url.origin) {
    return url.href;
  }

  const pathname = value.split(/[?#]/, 1)[0];
  if (/^(?!\/|[a-z][a-z\d+.-]*:).*\.mdx?$/i.test(pathname)) {
    const source = path.resolve(path.dirname(page.file), decodeURIComponent(pathname));
    const target = bySource.get(source);
    if (!target) {
      throw new Error(`Markdown link has no published page: ${value}`);
    }
    url.pathname = target.url.pathname;
  } else {
    url.pathname = byRoute.get(url.pathname.replace(/\/$/, '')) ?? url.pathname;
  }
  return url.href;
}

async function writeMarkdown(page, {siteConfig, outDir, bySource, byRoute}) {
  const {content} = await parseMarkdownFile({
    filePath: page.file,
    fileContent: await readFile(page.file, 'utf8'),
    parseFrontMatter: siteConfig.markdown.parseFrontMatter,
  });
  const tree = markdown.parse(escapeMarkdownHeadingIds(content));
  tree.children = plainBlocks(tree.children);
  visit(tree, 'heading', (node) => {
    const last = node.children.at(-1);
    if (last?.type === 'text') {
      last.value = parseMarkdownHeadingId(last.value).text;
    }
  });
  visit(tree, ['link', 'image', 'definition'], (node) => {
    node.url = canonicalLink(node.url, page, bySource, byRoute);
  });

  if (tree.children[0]?.type !== 'heading' || tree.children[0].depth !== 1) {
    tree.children.unshift({type: 'heading', depth: 1, children: [{type: 'text', value: page.title}]});
  }
  const source = paragraph('Source: ');
  source.children.push({type: 'link', url: page.url.href, children: [{type: 'text', value: page.url.href}]});
  tree.children.splice(1, 0, source);

  const contentMarkdown = markdown.stringify(tree);
  const file = path.join(outDir, decodeURIComponent(page.markdownUrl.pathname));
  await mkdir(path.dirname(file), {recursive: true});
  await writeFile(file, contentMarkdown);
  return contentMarkdown;
}

function docIds(item) {
  if (item.type === 'doc') {
    return [item.id];
  }
  if (item.type === 'category') {
    const landingPage = item.link?.type === 'doc' ? [item.link.id] : [];
    return [...landingPage, ...item.items.flatMap(docIds)];
  }
  return [];
}

function groupDocs(versions, pages) {
  const groups = new Map();
  for (const {docs, sidebars} of versions) {
    const byId = new Map(docs.map((doc) => [doc.id, doc]));
    for (const item of sidebars.docs) {
      for (const id of docIds(item)) {
        const doc = byId.get(id);
        const page = pages.get(doc?.permalink);
        if (page && !page.group) {
          page.group = item.type === 'category' ? item.label : 'Overview';
          const entries = groups.get(page.group) ?? [];
          entries.push(page);
          groups.set(page.group, entries);
        }
      }
    }
  }
  const otherDocs = [...pages.values()].filter((page) => !page.group);
  if (otherDocs.length) {
    groups.set('Other documentation', otherDocs);
  }
  return groups;
}

function indexEntry(page) {
  const title = page.title.replace(/[\\[\]]/g, '\\$&');
  const description = page.description.trim().replace(/\s+/g, ' ');
  return `- [${title}](${page.markdownUrl})${description ? `: ${description}` : ''}`;
}

function homepageMarkdown(homepage, home, byRoute) {
  const link = ({title, description, to}) => {
    const url = new URL(byRoute.get(to) ?? to, home);
    return `- [${title}](${url})${description ? `: ${description}` : ''}`;
  };
  return [
    `# ${homepage.title}`,
    `Source: [${home}](${home})`,
    homepage.lead,
    homepage.actions.map(link).join('\n'),
    `## ${homepage.example.title}`,
    `\`\`\`bash\n${homepage.example.command}\n\`\`\``,
    `\`\`\`csv\n${homepage.example.output}\n\`\`\``,
    '## Guides',
    homepage.guides.map(link).join('\n'),
    '## Reference',
    homepage.references.map(link).join('\n'),
  ].join('\n\n') + '\n';
}

export default function crawlerContent() {
  let versions;
  return {
    name: 'lynxdb-crawler-content',
    allContentLoaded({allContent}) {
      versions = allContent['docusaurus-plugin-content-docs'].default.loadedVersions;
    },
    async postBuild({siteConfig, siteDir, outDir, routes, routesBuildMetadata}) {
      const canonicalPath = (pathname) => applyTrailingSlash(pathname, siteConfig);
      const routePaths = flattenRoutes(routes).map(({path: pathname}) => canonicalPath(pathname));
      const byRoute = new Map(routePaths.map((pathname) => [pathname.replace(/\/$/, ''), pathname]));
      const home = new URL(siteConfig.baseUrl, siteConfig.url);
      const sitemap = new URL('sitemap.xml', home);
      const robots = siteConfig.noIndex
        ? 'User-agent: *\nDisallow: /\n'
        : `User-agent: *\nAllow: /\n\nSitemap: ${sitemap}\n`;
      await writeFile(path.join(outDir, 'robots.txt'), robots);

      // Serve permanent redirects for the extensionless URLs that Pages redirected.
      const redirects = routePaths.filter((pathname) => pathname.endsWith('/')).flatMap((pathname) => [
        ...(pathname === '/' ? [] : [`${pathname.slice(0, -1)} ${pathname} 308`]),
        `${pathname}index.html ${pathname} 308`,
      ]);
      // The project website hosts the blog and its RSS feed.
      redirects.push(
        '/blog https://lynxdb.org/blog/ 308',
        '/blog/rss.xml https://lynxdb.org/blog/feed.xml 308',
        '/blog/atom.xml https://lynxdb.org/blog/feed.xml 308',
        '/blog/* https://lynxdb.org/blog/ 308',
      );
      await writeFile(path.join(outDir, '_redirects'), `${[...new Set(redirects)].join('\n')}\n`);
      const headers = [
        '/*\n  Access-Control-Allow-Origin: *\n  X-Content-Type-Options: nosniff\n  Referrer-Policy: strict-origin-when-cross-origin',
        '/assets/*\n  Cache-Control: public, max-age=31536000, immutable',
        `/*.md\n  Content-Type: text/markdown; charset=utf-8\n  Link: <${siteConfig.url}/:splat/>; rel="canonical"`,
        `/index.md\n  ! Link\n  Link: <${home}>; rel="canonical"`,
        '/openapi.yaml\n  Content-Type: application/yaml',
        '/search/\n  X-Robots-Tag: noindex, follow',
        'https://:worker.:subdomain.workers.dev/*\n  X-Robots-Tag: noindex',
      ];
      await writeFile(path.join(outDir, '_headers'), `${headers.join('\n\n')}\n`);

      if (siteConfig.noIndex) {
        return;
      }

      const [homeContent, apiYaml] = await Promise.all([
        readFile(path.join(siteDir, 'homepage.json'), 'utf8'),
        readFile(path.join(outDir, 'api', 'swagger.yaml'), 'utf8'),
      ]);
      const landing = homepageMarkdown(JSON.parse(homeContent), home, byRoute);
      const api = yaml.load(apiYaml, {schema: yaml.JSON_SCHEMA});

      const pages = new Map();
      const bySource = new Map();
      const documents = versions.flatMap(({docs}) => docs);
      for (const doc of documents) {
        const url = new URL(canonicalPath(doc.permalink), siteConfig.url);
        const file = path.join(siteDir, aliasedSitePathToRelativePath(doc.source));
        const markdownUrl = new URL(`${url.pathname.replace(/\/$/, '')}.md`, url);
        const page = {...doc, file, url, markdownUrl};
        bySource.set(file, page);
        if (!doc.unlisted && !routesBuildMetadata[doc.permalink]?.noIndex) {
          pages.set(doc.permalink, page);
        }
      }

      const introduction = [
        '# LynxDB',
        '> LynxDB is an open-source log analytics database. Run OpenSearch Piped Processing Language (PPL) queries on local files and standard input, or ingest and query logs on a persistent server.',
      ];
      const groups = groupDocs(versions, pages);
      const index = [...introduction];
      index.push(`## Documentation home\n\n- [LynxDB documentation](${new URL('index.md', home)}): Guides, reference links and a first query example.`);
      for (const [title, entries] of groups) {
        index.push(`## ${title}\n\n${entries.map(indexEntry).join('\n')}`);
      }
      index.push('## Optional\n\n' + [
        `- [Complete documentation](${new URL('llms-full.txt', home)}): All listed pages in a single Markdown file.`,
        `- [OpenAPI JSON](${new URL('openapi.json', home)}): REST API endpoints and schemas.`,
        `- [OpenAPI YAML](${new URL('openapi.yaml', home)}): The same API contract in YAML.`,
        '- [LynxDB website and blog index](https://lynxdb.org/llms.txt): Product site and articles.',
      ].join('\n'));

      const full = await Promise.all([...groups.values()].flat().map(async (page) => {
        try {
          return await writeMarkdown(page, {siteConfig, outDir, bySource, byRoute});
        } catch (error) {
          throw new Error(`Could not export Markdown for ${page.source}`, {cause: error});
        }
      }));
      await Promise.all([
        writeFile(path.join(outDir, 'index.md'), landing),
        writeFile(path.join(outDir, 'openapi.yaml'), apiYaml),
        writeFile(path.join(outDir, 'openapi.json'), `${JSON.stringify(api, null, 2)}\n`),
        writeFile(path.join(outDir, 'llms.txt'), `${index.join('\n\n')}\n`),
        writeFile(path.join(outDir, 'llms-full.txt'), `${[landing, ...full].join('\n---\n\n')}`),
      ]);
    },
  };
}
