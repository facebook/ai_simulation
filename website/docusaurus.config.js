/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

const fs = require('fs');
const path = require('path');

const repoUrl = 'https://github.com/facebook/ai_simulation';
const repoRoot = path.resolve(__dirname, '..');
const docsDir = path.join(repoRoot, 'packsim');
// The pages are PackSim's own Markdown files, rendered where they live, so the
// site cannot drift from what GitHub shows beside the code.
const docFiles = ['README.md', 'transport/DESIGN.md'];

// A URL with a scheme (`https:`, `mailto:`), a fragment-only link or a
// site-absolute path: none of them is a relative link to a file.
const notRelative = /^(?:[a-z][a-z0-9+.-]*:|#|\/)/i;

/**
 * A remark plugin that points a relative link to a file the site does not
 * render -- `../LICENSE` in packsim/README.md, say -- at that file on GitHub,
 * instead of at a page that does not exist. Links between rendered pages are
 * left for Docusaurus to resolve.
 *
 * The site is built from the published repository, so a link to a file that
 * is not in it fails the build rather than becoming a GitHub 404. A definition
 * an image uses points at the file's raw form instead: Docusaurus bundles an
 * inline image, but not one reached through a definition.
 */
function linkFilesToGitHub() {
  const pages = new Set(docFiles.map((file) => path.join(docsDir, file)));
  const collectImageLabels = (tree) => {
    const labels = new Set();
    const collect = (node) => {
      if (node.type === 'imageReference') {
        labels.add(node.identifier);
      }
      for (const child of node.children ?? []) {
        collect(child);
      }
    };
    collect(tree);
    return labels;
  };
  const rewriteFileLink = (node, from, imageLabels) => {
    const [, target, suffix] = /^([^?#]*)(.*)$/.exec(node.url);
    let decoded;
    try {
      decoded = decodeURIComponent(target);
    } catch {
      throw new Error(`${from}: the link to ${node.url} is not a valid URL`);
    }
    const resolved = path.resolve(path.dirname(from), decoded);
    if (target === '' || pages.has(resolved)) {
      return;
    }
    const repoRelativePath = path.relative(repoRoot, resolved);
    const isOutsideRepo =
      repoRelativePath === '..' ||
      repoRelativePath.startsWith(`..${path.sep}`) ||
      path.isAbsolute(repoRelativePath);
    if (isOutsideRepo || !fs.existsSync(resolved)) {
      throw new Error(
        `${from}: the link to ${node.url} names a file that is not in the repository`,
      );
    }
    const kind =
      node.type === 'definition' && imageLabels.has(node.identifier)
        ? 'raw'
        : fs.statSync(resolved).isDirectory()
          ? 'tree'
          : 'blob';
    const urlPath = repoRelativePath
      .split(path.sep)
      .map((part) => encodeURIComponent(part))
      .join('/');
    node.url = `${repoUrl}/${kind}/main/${urlPath}${suffix}`;
  };
  const visit = (node, from, imageLabels) => {
    if (
      (node.type === 'link' || node.type === 'definition') &&
      !notRelative.test(node.url)
    ) {
      rewriteFileLink(node, from, imageLabels);
    }
    for (const child of node.children ?? []) {
      visit(child, from, imageLabels);
    }
  };
  return (tree, file) => visit(tree, file.path, collectImageLabels(tree));
}

/** @type {import('@docusaurus/types').Config} */
module.exports = {
  title: 'PackSim',
  tagline: 'A packet-level, discrete-event simulator for AI training fabrics',
  url: 'https://facebook.github.io',
  baseUrl: '/ai_simulation/',
  organizationName: 'facebook',
  projectName: 'ai_simulation',
  trailingSlash: false,

  onBrokenLinks: 'throw',
  // A link to a heading that has since been renamed still lands on the right
  // page, and nothing checks headings before GitHub builds the site, so it
  // warns instead of failing the build.
  onBrokenAnchors: 'warn',
  markdown: {
    // `.md` files are read as CommonMark rather than MDX, so a page renders as
    // it does on GitHub: a `{` or `<` in prose is text, not JSX.
    format: 'detect',
    hooks: {
      onBrokenMarkdownLinks: 'throw',
    },
  },

  presets: [
    [
      'classic',
      {
        docs: {
          path: docsDir,
          include: docFiles,
          routeBasePath: '/',
          sidebarPath: require.resolve('./sidebars.js'),
          beforeDefaultRemarkPlugins: [linkFilesToGitHub],
        },
        blog: false,
        pages: false,
      },
    ],
  ],

  themeConfig: {
    navbar: {
      title: 'PackSim',
      items: [
        {type: 'doc', docId: 'README', label: 'Overview', position: 'left'},
        {
          type: 'doc',
          docId: 'transport/DESIGN',
          label: 'Transport design',
          position: 'left',
        },
        {
          href: `${repoUrl}/blob/main/CONTRIBUTING.md`,
          label: 'Contributing',
          position: 'right',
        },
        {href: repoUrl, label: 'GitHub', position: 'right'},
      ],
    },
    footer: {
      style: 'dark',
      links: [
        {
          title: 'Project',
          items: [
            {label: 'GitHub', href: repoUrl},
            {
              label: 'Contributing',
              href: `${repoUrl}/blob/main/CONTRIBUTING.md`,
            },
            {
              label: 'Code of Conduct',
              href: `${repoUrl}/blob/main/CODE_OF_CONDUCT.md`,
            },
            {label: 'License', href: `${repoUrl}/blob/main/LICENSE`},
          ],
        },
        {
          title: 'Legal',
          // Meta's open source websites carry all four of these links. Do not
          // remove them.
          items: [
            {label: 'Privacy', href: 'https://opensource.fb.com/legal/privacy/'},
            {label: 'Terms', href: 'https://opensource.fb.com/legal/terms/'},
            {
              label: 'Data Policy',
              href: 'https://opensource.fb.com/legal/data-policy/',
            },
            {
              label: 'Cookie Policy',
              href: 'https://opensource.fb.com/legal/cookie-policy/',
            },
          ],
        },
      ],
      logo: {
        alt: 'Meta Open Source',
        src: 'img/meta_opensource_logo_negative.svg',
        href: 'https://opensource.fb.com',
        width: 300,
      },
      copyright: `Copyright © ${new Date().getFullYear()} Meta Platforms, Inc. Built with Docusaurus.`,
    },
    prism: {
      additionalLanguages: ['bash', 'cmake'],
    },
  },
};
