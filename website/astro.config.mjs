// @ts-check
import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';

export default defineConfig({
  site: 'https://upgrid.rs',
  devToolbar: { enabled: false },
  integrations: [
    starlight({
      title: 'UpGrid',
      description: 'Run and operate a distributed UpGrid service-monitoring Cluster.',
      logo: { src: './src/assets/logo.svg', alt: 'UpGrid' },
      customCss: ['./src/styles/upgrid.css'],
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
            { label: 'Install UpGrid', slug: 'getting-started/installation' },
            { label: 'Start your first Node', slug: 'getting-started/first-node' },
          ],
        },
        {
          label: 'Operate',
          items: [
            { label: 'Join a Cluster', slug: 'guides/join-cluster' },
            { label: 'Monitor services', slug: 'guides/targets' },
            { label: 'Send notifications', slug: 'guides/notifications' },
            { label: 'How UpGrid works', slug: 'guides/architecture' },
          ],
        },
        {
          label: 'Reference',
          items: [
            { label: 'Configuration', slug: 'reference/configuration' },
            { label: 'Deployment', slug: 'reference/deployment' },
            { label: 'HTTP API', slug: 'reference/api' },
          ],
        },
      ],
    }),
  ],
});
