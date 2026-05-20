## Contributing

1. Fork the repository and create a feature branch.
2. Keep changes focused and document behavior changes in `README.md` when needed.
3. Verify the stack locally before opening a pull request:

```bash
docker compose config
docker compose build migrator
```

4. Open a pull request with a clear summary, deployment notes, and any manual verification steps.

## Development Notes

- Copy `.env.example` to `.env` for local development.
- Runtime data under `alist/data/` and `app/data/` is intentionally ignored.
- Do not commit secrets, cookies, refresh tokens, or generated database files.
