/// <reference types="vitest" />
import { defineConfig, type Plugin } from "vite";
import react from "@vitejs/plugin-react";
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import path from "path";

const host = process.env.TAURI_DEV_HOST;

const CSP_REPORT_PATH = "/__csp-report";

/** The policy from tauri.conf.json, plus what a dev page needs on top. */
function devCsp(html: string): string {
  const conf = JSON.parse(
    readFileSync(path.resolve(__dirname, "src-tauri/tauri.conf.json"), "utf8"),
  );
  const directives: Record<string, string> = { ...conf.app.security.csp };
  // Like Tauri does for the bundle, allow the page's own inline scripts
  // by hash (in dev this includes the React Fast Refresh preamble).
  const inlineScripts = html.matchAll(/<script\b(?![^>]*\bsrc=)[^>]*>([\s\S]*?)<\/script>/gi);
  for (const [, body] of inlineScripts) {
    // Browsers hash the script text after HTML newline normalisation, so a
    // CRLF checkout (Windows) must be hashed with LF line endings.
    const text = body.replace(/\r\n?/g, "\n");
    const hash = createHash("sha256").update(text).digest("base64");
    directives["script-src"] += ` 'sha256-${hash}'`;
  }
  // HMR websocket.
  directives["connect-src"] += " ws://localhost:* ws://127.0.0.1:*";
  if (host) directives["connect-src"] += ` ws://${host}:1421`;
  directives["report-uri"] = CSP_REPORT_PATH;
  return Object.entries(directives)
    .map(([name, sources]) => `${name} ${sources}`)
    .join("; ");
}

/**
 * Serve the production Content Security Policy in dev too.
 *
 * Tauri only attaches `app.security.csp` to assets it serves itself, so
 * `tauri dev` (which loads this dev server) and the Playwright e2e suite
 * would otherwise run with no CSP at all, and anything the packaged app
 * blocks would only show up after a release build. This sends the same
 * policy as a response header on every HTML page the dev server serves,
 * and prints every violation the browser reports in the dev server log.
 */
function tauriCspInDev(): Plugin {
  return {
    name: "tablio:tauri-csp-in-dev",
    apply: "serve",
    configureServer(server) {
      server.middlewares.use(CSP_REPORT_PATH, (req, res) => {
        let body = "";
        req.on("data", (chunk) => (body += chunk));
        req.on("end", () => {
          let report: Record<string, unknown> = {};
          try {
            report = JSON.parse(body)["csp-report"] ?? {};
          } catch {}
          server.config.logger.warn(
            `[csp] ${report["effective-directive"] ?? report["violated-directive"]} blocked ` +
              `${report["blocked-uri"]} (${report["source-file"] ?? report["document-uri"] ?? "?"}` +
              `:${report["line-number"] ?? "?"})`,
            { timestamp: true },
          );
          res.statusCode = 204;
          res.end();
        });
      });
      server.middlewares.use((req, res, next) => {
        // Only page navigations can return HTML. Leaving every other
        // response (modules, the multi-megabyte Monaco bundle) untouched
        // also avoids a Node crash on Windows ("Lazy deopt after a fast API
        // call") when a wrapped res.end sends a very large string.
        if (!String(req.headers.accept ?? "").includes("text/html")) {
          next();
          return;
        }
        const end = res.end.bind(res);
        res.end = ((chunk?: unknown, ...rest: unknown[]) => {
          const type = String(res.getHeader("Content-Type") ?? "");
          if (
            chunk != null &&
            type.startsWith("text/html") &&
            !res.headersSent &&
            !res.hasHeader("Content-Security-Policy")
          ) {
            res.setHeader("Content-Security-Policy", devCsp(String(chunk)));
          }
          return (end as (...args: unknown[]) => typeof res)(chunk, ...rest);
        }) as typeof res.end;
        next();
      });
    },
  };
}

export default defineConfig(async () => ({
  plugins: [react(), tauriCspInDev()],
  test: {
    globals: true,
    environment: "jsdom",
    setupFiles: ["./src/test/setup.ts"],
    include: ["src/**/*.{test,spec}.{ts,tsx}"],
    coverage: {
      provider: "v8",
      reporter: ["text", "text-summary", "json", "json-summary", "html", "lcov"],
      exclude: ["node_modules/", "src/test/", "**/*.d.ts", "**/*.config.*"],
    },
  },
  resolve: {
    alias: {
      "@": path.resolve(__dirname, "./src"),
    },
  },
  build: {
    chunkSizeWarningLimit: 2200,
    rolldownOptions: {
      output: {
        codeSplitting: {
          // Monaco is bundled (see src/lib/monacoSetup.ts); keep it in its
          // own chunk rather than tripling the size of the app chunk.
          groups: [{ name: "monaco", test: /node_modules[\\/]monaco-editor[\\/]/ }],
        },
      },
    },
  },
  clearScreen: false,
  server: {
    port: 1420,
    strictPort: true,
    host: host || false,
    hmr: host
      ? {
          protocol: "ws",
          host,
          port: 1421,
        }
      : undefined,
    watch: {
      ignored: ["**/src-tauri/**"],
    },
  },
}));
