import type {ReactNode} from 'react';
import clsx from 'clsx';
import type {Props} from '@theme/Navbar/ColorModeToggle';
import ColorModeToggle from '@theme-original/Navbar/ColorModeToggle';

export default function NavbarColorModeToggle({className}: Props): ReactNode {
  return <ColorModeToggle className={clsx(className, 'lynx-theme-toggle')} />;
}
