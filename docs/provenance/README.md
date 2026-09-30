# Provenance

`release-hashes.txt` lists the signed tags of the first and the current
release with their commit and source-archive hashes. `release-hashes.txt.ots`
is an [OpenTimestamps](https://opentimestamps.org) proof that the file, and
so the code it names, existed no later than the Bitcoin block it is attached
to. The commit dates, the signatures on the tags and the entries in
`sum.golang.org` are the earlier evidence; this proof does not depend on any
of them, nor on GitHub.

The proof is attested in Bitcoin blocks 969313 and 969317 (2026-09-30).

`ots verify` checks the block headers against a local Bitcoin Core node;
without one, drop the two files into https://opentimestamps.org, or read
the proof and compare the merkle roots it names with any block explorer:

```bash
uvx --from opentimestamps-client ots info docs/provenance/release-hashes.txt.ots
```

Check that the hashes still match the tags:

```bash
git rev-parse v0.1.0 v0.1.0^{commit}
git archive --format=tar v0.1.0 | shasum -a 256
```
