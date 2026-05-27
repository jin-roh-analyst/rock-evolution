# Rock Evolution Web Dashboard

Static Next.js dashboard for the Rock Evolution project.

## Local Development

```bash
npm install
npm run dev
```

## Data

The dashboard reads committed static JSON files from `public/data`.

When the source CSV changes, regenerate the JSON locally:

```bash
npm run data
```

Then commit the updated files in `public/data`.

## Build

Use this for normal production builds and Vercel:

```bash
npm run build
```

Use this locally when you want to regenerate data and build in one command:

```bash
npm run build:with-data
```

## Vercel

- Root Directory: `web`
- Build Command: `npm run build`
- Install Command: `npm install`
- Output Directory: default Next.js setting

No Python or pandas setup is required on Vercel because the analytics outputs are committed as static JSON.
