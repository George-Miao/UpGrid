// @ts-check
import { defineConfig } from 'astro/config';
import { unified } from '@astrojs/markdown-remark';
import starlight from '@astrojs/starlight';

/** @typedef {{ type: string, tagName?: string, properties?: Record<string, unknown>, children?: MarkdownNode[] }} MarkdownNode */
/** @typedef {{ children: MarkdownNode[] }} MarkdownParent */

/** @param {MarkdownParent} parent */
function wrapTables(parent) {
  for (let index = 0; index < parent.children.length; index += 1) {
    const child = parent.children[index];
    if (child.type !== 'element') continue;

    if (child.tagName === 'table') {
      parent.children[index] = {
        type: 'element',
        tagName: 'div',
        properties: { className: ['table-frame'] },
        children: [child],
      };
      continue;
    }

    if (child.children) wrapTables({ children: child.children });
  }
}

function rehypeTableFrames() {
  return wrapTables;
}

export default defineConfig({
  site: 'https://upgrid.rs',
  devToolbar: { enabled: false },
  markdown: { processor: unified({ rehypePlugins: [rehypeTableFrames] }) },
  integrations: [
    starlight({
      title: 'UpGrid',
      description: 'Run and operate a distributed UpGrid service-monitoring cluster.',
      logo: { src: './src/assets/logo.svg', alt: 'UpGrid' },
      customCss: ['./src/styles/upgrid.css'],
      expressiveCode: {
        defaultProps: {
          wrap: true,
          overridesByLang: {
            'bash,sh,shell,zsh': { wrap: false },
          },
        },
      },
      components: {
        Footer: './src/components/Footer.astro',
      },
      head: [
        {
          tag: 'meta',
          attrs: {
            property: 'og:image',
            content: 'https://upgrid.rs/opengraph.png',
          },
        },
        {
          tag: 'meta',
          attrs: { property: 'og:image:type', content: 'image/png' },
        },
        {
          tag: 'meta',
          attrs: { property: 'og:image:width', content: '1200' },
        },
        {
          tag: 'meta',
          attrs: { property: 'og:image:height', content: '630' },
        },
        {
          tag: 'meta',
          attrs: {
            property: 'og:image:alt',
            content: 'UpGrid distributed service monitoring dashboard',
          },
        },
        {
          tag: 'meta',
          attrs: {
            name: 'twitter:image',
            content: 'https://upgrid.rs/opengraph.png',
          },
        },
        {
          tag: 'meta',
          attrs: {
            name: 'twitter:image:alt',
            content: 'UpGrid distributed service monitoring dashboard',
          },
        },
      ],
      editLink: {
        baseUrl: 'https://github.com/George-Miao/UpGrid/edit/main/website/',
      },
      lastUpdated: true,
      social: [{ icon: 'github', label: 'GitHub', href: 'https://github.com/George-Miao/UpGrid' }],
      sidebar: [
        {
          label: 'Start here',
          items: [
            { label: 'Overview', link: '/' },
            { label: 'Single-node setup', slug: 'getting-started/first-node' },
            { label: 'Multi-node setup', slug: 'getting-started/multi-node' },
          ],
        },
        {
          label: 'Operate',
          items: [
            { label: 'Network setup', slug: 'guides/network-setup' },
            {
              label: 'Cluster hardening',
              slug: 'guides/cluster-hardening',
            },
            { label: 'Add a node', slug: 'guides/join-cluster' },
            { label: 'Monitor services', slug: 'guides/targets' },
            { label: 'Send notifications', slug: 'guides/notifications' },
            { label: 'How UpGrid works', slug: 'guides/architecture' },
            { label: 'Recipes', slug: 'guides/recipes' },
          ],
        },
        {
          label: 'Reference',
          items: [
            { label: 'Configuration', slug: 'reference/configuration' },
            { label: 'Docker', slug: 'reference/docker' },
            { label: 'Up protocol', slug: 'reference/up-protocol' },
            { label: 'Script assertions', slug: 'reference/script-assertions' },
            { label: 'HTTP API', slug: 'reference/api' },
          ],
        },
      ],
    }),
  ],
});
