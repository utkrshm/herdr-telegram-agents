---
name: telegram-send-file
description: Send user-selected local images or files to the Telegram topic for the requesting Herdr pane with `herdr-tg send-image` or `send-file`. Use when the user asks to send, share, or attach a file or image to Telegram through Herdr.
---

# Send a file to Telegram through Herdr

Use the installed `/home/utkarsh/.local/bin/herdr-tg` CLI only when the user explicitly asks to send local file(s) or image(s) to Telegram. This performs an external send: confirm the requested files and destination intent before invoking it. Never send anything merely as a test.

## Choose the destination safely

- For the requesting pane, let the CLI use `HERDR_PANE_ID` from the current Herdr invocation environment. Do not guess a pane ID, infer one from unrelated context, or target another pane.
- An explicit `--pane <id>` is allowed only when the user clearly names or confirms that exact pane. If the intent or destination is unclear, ask before sending.
- If `HERDR_PANE_ID` is absent and no user-confirmed pane is provided, do not run the send command; explain that Herdr pane context is required.

## Choose the command

- Use `send-image` for images the user wants delivered as Telegram photos. Images over 10,000,000 bytes are automatically sent as documents instead; tell the user this fallback if relevant. Non-image files are not silently treated as photos.
- Use `send-file` when the user wants the original document/file, including when they explicitly want image bytes as a document.
- Both commands accept one or more paths. Each file is handled independently; multiple images are not sent as a Telegram album. Quote every path so spaces and shell metacharacters remain part of the filename.
- The CLI sends the original file bytes; do not edit, convert, redact, rename, or substitute user files. Do not read or disclose file contents or secrets just to send them. Never print credentials or include secrets in shell arguments.
- Files must be regular, non-empty, and at most 50,000,000 bytes (the CLI labels this 50 MB). Do not try to bypass the cap by splitting or altering a file.

## Run and report

Use the exact executable and syntax below. Without `--pane`, preserve the existing `HERDR_PANE_ID` environment; do not replace it with a guessed value.

```sh
"/home/utkarsh/.local/bin/herdr-tg" send-image "./photos/diagram 1.png"
"/home/utkarsh/.local/bin/herdr-tg" send-image "./photos/first.png" "./photos/second.png"
"/home/utkarsh/.local/bin/herdr-tg" send-file "./reports/final report.pdf"
"/home/utkarsh/.local/bin/herdr-tg" send-file --pane "pane-id-confirmed-by-user" "./reports/final report.pdf"
```

The CLI has no `--help` option for these subcommands; their usage is `send-image [--pane <id>] <file>...` and `send-file [--pane <id>] <file>...`.

- Inspect the command result. Success prints one `path: sent ...` line per file to stdout and exits 0. Per-file failures print `path: failed: ...` to stdout; any failure makes the overall exit code 1, even if other files succeeded. Missing pane or files prints usage to stderr and exits 2. Other setup/daemon failures exit 1.
- Summarize exactly which files succeeded and failed based on the output. Do not claim success if the output is absent or unclear.
- The CLI does not automatically retry Telegram uploads. If it reports an error, times out, or the result is ambiguous, do not blindly retry: an upload may have reached Telegram despite an uncertain response. Explain the uncertainty and ask the user to check the intended topic before considering another attempt.
