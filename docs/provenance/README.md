# Provenance

`release-hashes.txt` lists the signed tags of the first and the current
release with their commit and source-archive hashes. `release-hashes.txt.ots`
is an [OpenTimestamps](https://opentimestamps.org) proof that the file, and
so the code it names, existed no later than the Bitcoin block it is attached
to. The commit dates, the signatures on the tags and the entries in
`sum.golang.org` are the earlier evidence; this proof does not depend on any
of them, nor on GitHub.

Verify, without installing anything:

```bash
uvx --from opentimestamps-client ots verify docs/provenance/release-hashes.txt.ots
```

Check that the hashes still match the tags:

```bash
git rev-parse v0.1.0 v0.1.0^{commit}
git archive --format=tar v0.1.0 | shasum -a 256
```

The proof was made a few days after v0.3.0 and upgraded once the Bitcoin
attestation was available. If `ots verify` reports a pending attestation,
run `ots upgrade` on the file first.
