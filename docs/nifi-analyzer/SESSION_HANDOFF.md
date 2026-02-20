# NiFi Flow Analyzer — Session Handoff

## What This Is
A browser-based tool that converts Apache NiFi flows (XML/JSON) to Databricks PySpark notebooks. Users upload a NiFi flow file and get a complete migration package: analysis, assessment, generated notebook, workflow JSON, validation, and value analysis.

## Live URL
**https://dw425.github.io/DAIS_SEG/nifi-analyzer/**

GitHub Pages serves from `/docs` on `main` branch. The `index.html` at `docs/nifi-analyzer/index.html` is a self-contained single-file build (505KB, all JS/CSS inlined).

## Repo & Location
- **Repo**: `dw425/DAIS_SEG` on GitHub
- **Path**: `docs/nifi-analyzer/`
- **Branch**: `main`

## Architecture

### Build System
- **Vite** + `vite-plugin-singlefile` → outputs single `index.html`
- **Vitest** (jsdom) for unit tests
- Dev entry: `index.dev.html` (has `<script type="module" src="./src/main.js">`)
- Build output: `dist/index.dev.html` → copied to `index.html` for GitHub Pages
- `vite.config.js` uses `rollupOptions.input: 'index.dev.html'`

### Build Commands
```bash
cd docs/nifi-analyzer
npm install                    # First time only
./node_modules/.bin/vite build # Build production bundle
cp dist/index.dev.html index.html  # Deploy to GitHub Pages path
./node_modules/.bin/vitest run # Run 93 unit tests
```

### Dev Server (for local development)
```bash
cd docs/nifi-analyzer
./node_modules/.bin/vite --port 5174
# IMPORTANT: must run from docs/nifi-analyzer/ directory, not repo root
# Dev server uses index.dev.html (the entry with <script src="./src/main.js">)
```

### Module Structure (143 ES modules + 14 CSS + 16 tests = 173 files)
```
docs/nifi-analyzer/
  index.html                    ← Built single-file (GitHub Pages serves this)
  index.dev.html                ← Dev shell with <script type="module">
  vite.config.js                ← Build config (entry: index.dev.html)
  package.json
  src/
    main.js                     ← Bootstrap: imports, DOM wiring, window.* exports
    core/          (5 modules)  ← state, event-bus, config, pipeline, errors
    parsers/       (17 modules) ← XML, JSON, DDL, SQL, NEL engine (7 sub-modules)
    analyzers/     (12 modules) ← dep graph, systems, cycles, PHI, security, manifest
    mappers/       (20 modules) ← classifier, template resolver, 14 handler files
    generators/    (19 modules) ← notebook, workflow, cell builders, wrappers
    validators/    (7 modules)  ← 4-angle validation (intent, line, RE, function)
    reporters/     (8 modules)  ← migration report, final report, value analysis
    ui/            (22 modules) ← tabs, panels, file upload, tier diagram, charts
    security/      (4 modules)  ← HTML sanitizer, input validator, sensitive props
    constants/     (12 modules) ← NIFI_DATABRICKS_MAP (250+ processors), system maps
    utils/         (8 modules)  ← string, DOM, score, debounce, BFS, graph
  styles/          (14 CSS)     ← base, layout, components, metrics, tables, etc.
  test/            (16 tests)   ← 93 passing tests
  test-data/                    ← large_flow_1000.xml, events.xml
```

## 8-Step Pipeline
| Step | Tab | Function | Source |
|------|-----|----------|--------|
| 1 | Load Flow | `parseInput()` | `ui/step-handlers.js` → calls `parseFlow()` |
| 2 | Analyze | `runAnalysis()` | `ui/step-handlers.js` |
| 3 | Assess | `runAssessment()` | `ui/step-handlers.js` |
| 4 | Convert | `generateNotebook()` | `ui/step-handlers.js` |
| 5 | Report | `generateReport()` | `ui/step-handlers.js` |
| 6 | Final Report | `window.generateFinalReport()` | `main.js` → `reporters/final-report.js` |
| 7 | Validate | `window.runValidation()` | `main.js` → `validators/index.js` |
| 8 | Value Analysis | `window.runValueAnalysis()` | `main.js` → `reporters/value-analysis.js` |

Steps 1-5 are in `step-handlers.js` with proper ES module imports.
Steps 6-8 are wired as `window.*` functions in `main.js` (they need STATE injection).

## Key Design Patterns

### State Management
- `core/state.js` — reactive store with `getState()`, `setState()`, `resetState()`, `subscribe()`
- No global `window.STATE` — all state via module imports

### Mapper Dependencies
- `mapNiFiToDatabricks(nifi, deps)` requires a `deps` object with 8 external functions
- `mapNiFiToDatabricksAuto(nifi)` is a convenience wrapper that auto-injects deps
- All callers (step-handlers, main.js) import `mapNiFiToDatabricksAuto as mapNiFiToDatabricks`

### File Upload Flow
- `file-upload.js` stores content in module-level `uploadedContent` variable
- `handleFile()` returns a Promise (FileReader is async)
- `main.js` awaits `handleFile()` before calling `parseInput()`
- `parseInput()` calls `parseFlow(raw, filename)` which handles format detection + `_nifi` wrapping

### Build → Deploy Pipeline
1. Edit source in `src/` modules
2. `vite build` (uses `index.dev.html` as entry)
3. Copy `dist/index.dev.html` → `index.html`
4. Commit `index.html` + changed source files
5. Push to `main` → GitHub Pages auto-deploys

## Test Results (1000-processor flow)
| Metric | Value |
|--------|-------|
| Processors parsed | 1000 |
| Connections | 1148 |
| Processors mapped | 1000/1000 (100%) |
| Average confidence | 92% |
| Notebook cells | 1023 |
| Validation score | 94% |
| Intent match | 100% |
| Line coverage | 83% |
| Unit tests | 93/93 passing |

## Recent Commits (latest first)
- `13ff36b` fix: wire steps 6-8 (Final Report, Validation, Value Analysis) on window
- `824b148` fix: async file read race + GitHub Pages build pipeline
- `559ef9a` build: replace dev shell with production single-file bundle for GitHub Pages
- `786516d` fix: wire ES module imports in step-handlers, use parseFlow for correct _nifi wrapping
- `84b359b` feat: modular remodel — decompose 9,257-line monolith into 157 ES modules

## Known Issues / Remaining Work
1. **Expander onclick in generated buttons**: Download buttons in steps 4-5 still use `onclick="downloadNotebook()"` etc. — works because those functions ARE on `window`, but ideally should use event delegation
2. **`.gitignore`**: `dist/` is ignored but `index.dev.html` is tracked (intentional — it's the build entry)
3. **Two .bak files**: `index.html.bak` and `index.html.bak2` are untracked leftovers from the original monolith
4. **package-lock.json**: Untracked, should probably be committed for reproducible builds
5. **`.vite/` cache**: Untracked directory at repo root (created by npx vite), add to root .gitignore

## Original Monolith
The original `index.html` was 9,257 lines with all HTML, CSS, and JS inline. It has been decomposed into 173 files. The monolith backup exists as `index.html.bak`.

## Security Fixes Applied
- `eval()` removed — safe math parser in `parsers/nel/el-evaluator.js`
- `innerHTML` XSS — all user data goes through `escapeHTML()`
- Inline `onclick` removed from HTML — replaced with `addEventListener` and `data-expander-toggle` event delegation
- Sensitive properties masked in UI display
