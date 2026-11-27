import type {ReactNode} from 'react';
import {useDoc} from '@docusaurus/plugin-content-docs/client';
import DocItemMetadata from '@theme-original/DocItem/Metadata';
import MarkdownAlternate from '../../../components/MarkdownAlternate';

export default function Metadata(): ReactNode {
  const {metadata} = useDoc();
  return (
    <>
      <DocItemMetadata />
      <MarkdownAlternate permalink={metadata.permalink} unlisted={metadata.unlisted} />
    </>
  );
}
