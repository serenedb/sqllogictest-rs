# SQLLogicTest syntax highlighting

A VSCode extension that colors sqllogictest files in three groups:

| what | scope | default color |
| --- | --- | --- |
| SQL inside `statement` / `query` / `let` bodies | `meta.embedded.block.sql` + `source.sql` | whatever your theme uses for SQL |
| sqllogictest-rs directives and their arguments | `keyword.*` | the theme's keyword color |
| `#` comments | `comment.line.number-sign.sqllogic` | the theme's comment color |

Everything else -- expected results under `----`, `system` command bodies --
stays at the default foreground on purpose.

## Install

Build the `.vsix` and install it, then reload the window:

```bash
tools/vscode-sqllogic/package.sh
code --install-extension tools/vscode-sqllogic/sqllogic-0.1.0.vsix
```

Over SSH the `code` CLI is only on `$PATH` inside VSCode's own terminal. From
anywhere else, call the server's copy:

```bash
"$(ls -td ~/.vscode-server/cli/servers/*/server | head -1)/bin/code-server" \
  --install-extension tools/vscode-sqllogic/sqllogic-0.1.0.vsix --force
```

**Copying or symlinking the folder into `~/.vscode-server/extensions/` does not
work.** Since VSCode 1.75 the extension host only loads what is listed in
`extensions.json` next to those folders, and nothing scans the directory for
strays -- the extension silently never loads and files stay Plain Text. Install
through the CLI so that manifest gets written.

`package.sh` builds the `.vsix` with `zip` rather than `@vscode/vsce`, which
needs a newer node than the system one on most boxes here. The extension is
declarative -- a grammar and a language config, no TypeScript -- so there is
nothing else to compile.

To uninstall:

```bash
code --uninstall-extension serenedb.sqllogic
```

## File association

`.slt` is claimed by extension. `.test` is too generic to claim outright, so it
is claimed only under `**/sqllogic/**` and `**/test/sql/**` -- in SereneDB that
covers `tests/sqllogic/` and the DuckDB extension suites under `third_party/`.
For anything else, add to your settings:

```json
"files.associations": { "**/my/other/tree/*.test": "sqllogic" }
```

## Grammar notes

The grammar follows `sqllogictest/src/parser.rs`:

- A `#` comment is only a comment in column 0 of a record boundary. A line
  starting with `#` inside a SQL body is SQL, and the grammar keeps it SQL.
- A record body runs until the first **empty** line (zero-length, not
  whitespace-only) or a `----` line -- so is the SQL region.
- `let` has no `----` form; its body ends at the empty line only.
- Multi-line error text under `----` may itself contain a single blank line.
  The grammar closes the result region there; the remaining lines fall back to
  the default foreground, which looks identical, and the next record resyncs.

To highlight SQL with a different grammar (a dedicated PostgreSQL extension,
say), change the one `{"include": "source.sql"}` in
`syntaxes/sqllogic.tmLanguage.json` to that grammar's scope name, and the
matching entry under `embeddedLanguages` in `package.json`.

Every directive token gets a scope under `keyword.` so any theme paints them as
one color, but the suffixes are specific (`keyword.control.record.sqllogic`,
`keyword.other.argument.sqllogic`, `keyword.control.results-delimiter.sqllogic`,
...) so you can split them apart with `editor.tokenColorCustomizations` if you
want more than three colors.
