FROM node:24-alpine

WORKDIR /app
COPY server.js package.json ./

ENV NODE_ENV=production
EXPOSE 8787

USER node
CMD ["node", "server.js"]
