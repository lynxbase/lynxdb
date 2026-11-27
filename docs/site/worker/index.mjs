function prefersMarkdown(accept = '') {
  let markdown = 0;
  let html = 0;
  for (const item of accept.toLowerCase().split(',')) {
    const [type, ...parameters] = item.trim().split(';');
    const q = parameters.map((field) => field.trim()).find((field) => /^q\s*=/.test(field));
    const weight = q === undefined ? 1 : Number(q.split('=')[1]);
    if (!(weight > 0 && weight <= 1)) continue;
    if (type.trim() === 'text/markdown') markdown = Math.max(markdown, weight);
    if (type.trim() === 'text/html') html = Math.max(html, weight);
  }
  return markdown > 0 && markdown >= html;
}

function varyOnAccept(response) {
  const result = new Response(response.body, response);
  result.headers.append('Vary', 'Accept');
  if (result.status === 404) result.headers.set('X-Robots-Tag', 'noindex');
  return result;
}

export default {
  async fetch(request, {ASSETS}) {
    const url = new URL(request.url);
    const pathname = url.pathname;
    const isPage = pathname === '/' || (pathname.startsWith('/docs/') && pathname.endsWith('/'));
    if (!isPage || (request.method !== 'GET' && request.method !== 'HEAD')) {
      return ASSETS.fetch(request);
    }

    if (prefersMarkdown(request.headers.get('Accept') ?? '')) {
      url.pathname = pathname === '/' ? '/index.md' : `${pathname.slice(0, -1)}.md`;
      const markdown = await ASSETS.fetch(new Request(url, request));
      if (markdown.ok || markdown.status === 304) return varyOnAccept(markdown);
      await markdown.body?.cancel();
    }
    return varyOnAccept(await ASSETS.fetch(request));
  },
};
