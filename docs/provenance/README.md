# Provenance

`release-hashes.txt` lists the signed tags of the first and the current
release with their commit and source-archive hashes. `release-hashes.txt.ots`
is an [OpenTimestamps](https://opentimestamps.org) proof that the file, and
so the code it names, existed no later than the Bitcoin block it is attached
to. The commit dates, the signatures on the tags and the entries in
`sum.golang.org` are the earlier evidence; this proof does not depend on any
of them, nor on GitHub.

The file as it is now, with v0.1.0 and v0.5.1, was stamped on 2026-11-20
and is attested in Bitcoin blocks 976704, 976709 and 976724; the proof
that was pending in 1b9123d is upgraded with them.

The files as they were before are in the history with their proofs. The
one with v0.1.0 and v0.3.0 is attested in Bitcoin blocks 969313 and 969317
(2026-09-30) and stays the evidence for the first release; the one with
v0.1.0 and v0.5.0, in blocks 976616, 976629, 976646 and 976662:

```bash
c=3f775ce   # v0.1.0 and v0.3.0; be5e31c for v0.1.0 and v0.5.0
git show $c:docs/provenance/release-hashes.txt > release-hashes.txt
git show $c:docs/provenance/release-hashes.txt.ots > release-hashes.txt.ots
```

## Adding a release

After the tag `vX.Y.Z` is on `main`, with git 2.55.0 or the version the file
names:

```bash
git fetch origin --tags
tag=vX.Y.Z
echo "$tag tag=$(git rev-parse "$tag") commit=$(git rev-parse "$tag^{commit}")" \
  "archive_sha256=$(git archive --format=tar "$tag" | shasum -a 256 | cut -d' ' -f1)"
```

Put the line in `release-hashes.txt` in place of the previous current
release's, keeping the first release's line.

## Timestamping

The file changed, so its proof is made anew:

```bash
rm docs/provenance/release-hashes.txt.ots
uvx --from opentimestamps-client ots stamp docs/provenance/release-hashes.txt
# a few hours later, once the calendars have attached it to a block:
uvx --from opentimestamps-client ots upgrade docs/provenance/release-hashes.txt.ots
```

Commit `release-hashes.txt` and `release-hashes.txt.ots` together in a
`docs:` pull request, and the upgraded proof in a second one that names the
Bitcoin block(s) here and the commit of the file the proof replaced.

## Verifying

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
