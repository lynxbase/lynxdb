import type {ReactNode} from 'react';
import {translate} from '@docusaurus/Translate';
import DocItemTOCDesktop from '@theme-original/DocItem/TOC/Desktop';

export default function DesktopContents(): ReactNode {
  const label = translate({id: 'theme.TOCCollapsible.toggleButtonLabel', message: 'On this page'});

  return (
    <nav aria-label={label} className="docs-toc">
      <div className="docs-toc__label" aria-hidden="true">{label}</div>
      <DocItemTOCDesktop />
    </nav>
  );
}
