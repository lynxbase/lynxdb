import type {ReactNode} from 'react';
import {useBlogPost} from '@docusaurus/plugin-content-blog/client';
import BlogPostMetadata from '@theme-original/BlogPostPage/Metadata';
import MarkdownAlternate from '../../../components/MarkdownAlternate';

export default function Metadata(): ReactNode {
  const {metadata} = useBlogPost();
  return (
    <>
      <BlogPostMetadata />
      <MarkdownAlternate permalink={metadata.permalink} unlisted={metadata.unlisted} />
    </>
  );
}
