"""One-time, archived, SHA-guarded branch cleanup. No default-branch writes."""

import argparse
import json
import os
import re
import subprocess
import urllib.request
from pathlib import Path


def git(root, *args, check=True):
    return subprocess.run(
        ["git", "-C", str(root), *args], check=check, text=True,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )


class GitHub:
    def __init__(self, repo, token):
        self.base = "https://api.github.com/repos/" + repo
        self.token = token

    def get(self, path):
        request = urllib.request.Request(self.base + path, headers={
            "Authorization": "Bearer " + self.token,
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        })
        with urllib.request.urlopen(request, timeout=30) as response:
            return json.load(response)

    def pages(self, path):
        result = []
        for page in range(1, 100):
            batch = self.get(path + ("&" if "?" in path else "?") + f"per_page=100&page={page}")
            result.extend(batch)
            if len(batch) < 100:
                return result
        raise RuntimeError("pagination limit reached; nothing deleted")


def remote_ref(root, ref):
    lines = git(root, "ls-remote", "--refs", "origin", ref).stdout.splitlines()
    return next((line.split()[0] for line in lines if line.split()[1] == ref), None)


def integrated(root, sha, base):
    if git(root, "rev-parse", sha + "^{tree}").stdout == git(root, "rev-parse", base + "^{tree}").stdout:
        return True
    if git(root, "merge-base", "--is-ancestor", sha, base, check=False).returncode == 0:
        return True
    patches = git(root, "cherry", base, sha).stdout.splitlines()
    return bool(patches) and all(line.startswith("- ") for line in patches)


def run_cleanup(root, manifest, api, operation_branch, apply=False):
    repo = manifest["repository"]
    default = api.get("")["default_branch"]
    if default != manifest["defaultBranch"] or default == operation_branch:
        raise RuntimeError("default branch changed; nothing deleted")
    if not operation_branch.startswith("maintenance/cleanup-run-"):
        raise RuntimeError("unexpected operation branch; nothing deleted")
    head = git(root, "rev-parse", "HEAD").stdout.strip()
    if remote_ref(root, "refs/heads/" + operation_branch) != head:
        raise RuntimeError("operation branch moved; nothing deleted")
    git(root, "fetch", "--quiet", "origin", "refs/heads/" + default)
    base = git(root, "rev-parse", "FETCH_HEAD").stdout.strip()
    protected = {b["name"] for b in api.pages("/branches") if b.get("protected")}
    open_heads = {
        p["head"]["ref"] for p in api.pages("/pulls?state=open")
        if p["head"]["repo"] and p["head"]["repo"]["full_name"].lower() == repo.lower()
    }
    # Every candidate is a parent/ancestor of this archive commit, including
    # squash-equivalent histories that are not reachable from the default head.
    for item in manifest["delete"]:
        if not re.fullmatch("[0-9a-f]{40}", item["sha"]):
            raise RuntimeError("invalid candidate SHA")
        git(root, "check-ref-format", "refs/heads/" + item["name"])
        if git(root, "merge-base", "--is-ancestor", item["sha"], head, check=False).returncode:
            raise RuntimeError("candidate history missing from archive; nothing deleted")
    tag = "refs/tags/" + manifest["backupTag"]
    previous = remote_ref(root, tag)
    if previous not in (None, head):
        raise RuntimeError("backup tag already points elsewhere; nothing deleted")
    if apply and previous is None:
        git(root, "push", "origin", head + ":" + tag)
    if apply and remote_ref(root, tag) != head:
        raise RuntimeError("archive tag verification failed; nothing deleted")

    result = {"repository": repo, "backupTag": manifest["backupTag"],
              "archiveCommit": head, "deleted": [], "kept": []}
    for item in manifest["delete"]:
        name, sha = item["name"], item["sha"]
        ref = "refs/heads/" + name
        reason = None
        if name == default or name == operation_branch or name in protected:
            reason = "protected/default branch"
        elif name.startswith(("backup/", "release/", "deploy/")) or name in ("production", "staging"):
            reason = "release/deployment/backup branch"
        elif name in open_heads:
            reason = "open pull request"
        elif remote_ref(root, ref) != sha:
            reason = "branch changed or already removed"
        elif item["reason"] == "rejected-dependabot-major":
            pr = api.get("/pulls/" + str(item["pullRequest"]))
            if not (pr["state"] == "closed" and pr["user"]["login"] == "dependabot[bot]"
                    and pr["head"]["sha"] == sha and pr["head"]["ref"] == name
                    and pr["head"]["repo"]["full_name"].lower() == repo.lower()):
                reason = "rejected update no longer matches audited PR"
        elif not integrated(root, sha, base):
            reason = "contains changes not integrated in the default branch"
        if reason:
            result["kept"].append({**item, "note": reason})
            print("KEEP", name, reason, flush=True)
            continue
        if apply:
            # Atomic compare-and-delete: a push after inspection fails the lease.
            pushed = git(root, "push", "--force-with-lease=" + ref + ":" + sha,
                         "origin", ":" + ref, check=False)
            if pushed.returncode:
                result["kept"].append({**item, "note": "delete lease rejected"})
                print("KEEP", name, "delete lease rejected", flush=True)
                continue
        result["deleted"].append(item)
        print("DELETE" if apply else "WOULD DELETE", name, sha, flush=True)
    if apply:
        own = "refs/heads/" + operation_branch
        git(root, "push", "--force-with-lease=" + own + ":" + head, "origin", ":" + own)
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    manifest = json.loads(Path(".github/maintenance/cleanup-manifest.json").read_text())
    if manifest["repository"] != os.environ["GITHUB_REPOSITORY"]:
        raise RuntimeError("repository mismatch")
    result = run_cleanup(Path.cwd(), manifest,
                         GitHub(manifest["repository"], os.environ["GITHUB_TOKEN"]),
                         os.environ["GITHUB_REF_NAME"], args.apply)
    Path("cleanup-result.json").write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps({"deleted": len(result["deleted"]), "kept": len(result["kept"]),
                      "backupTag": result["backupTag"]}))
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a") as output:
            output.write(f"Deleted {len(result['deleted'])} audited branches; kept {len(result['kept'])}.\n\n")
            output.write(f"Archive: `{result['backupTag']}` at `{result['archiveCommit']}`.\n")
