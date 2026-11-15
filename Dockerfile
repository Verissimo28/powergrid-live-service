FROM node:24.22.1-bookworm-slim AS build

WORKDIR /app

RUN corepack enable && corepack prepare pnpm@10.0.0 --activate

COPY package.json pnpm-lock.yaml ./
RUN pnpm install --frozen-lockfile

COPY tsconfig.base.json tsconfig.json ./
COPY src ./src

RUN pnpm build
RUN pnpm prune --prod

FROM node:24.22.1-bookworm-slim AS runtime

ENV NODE_ENV=production
WORKDIR /app

COPY --from=build /app/package.json ./package.json
COPY --from=build /app/node_modules ./node_modules
COPY --from=build /app/dist ./dist

EXPOSE 3002

USER node

CMD ["node", "dist/main.js"]
EOFcat > /home/verissimo/powergrid-workspace/powergrid-live-service/Dockerfile <<'EOF'
FROM node:24.22.1-bookworm-slim AS build

WORKDIR /app

RUN corepack enable && corepack prepare pnpm@10.0.0 --activate

COPY package.json pnpm-lock.yaml ./
RUN pnpm install --frozen-lockfile

COPY tsconfig.base.json tsconfig.json ./
COPY src ./src

RUN pnpm build
RUN pnpm prune --prod

FROM node:24.22.1-bookworm-slim AS runtime

ENV NODE_ENV=production
WORKDIR /app

COPY --from=build /app/package.json ./package.json
COPY --from=build /app/node_modules ./node_modules
COPY --from=build /app/dist ./dist

EXPOSE 3002

USER node

CMD ["node", "dist/main.js"]
