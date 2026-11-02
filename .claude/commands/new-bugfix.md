# /new-bugfix

Create a new bugfix branch targeting the current release branch.

## Steps

1. Ask the user for:
   - The Jira ticket ID (e.g., `AMRIT-456`)
   - A short description of the bug being fixed (3–5 words, kebab-case)
   - Which release branch to target (default: latest `release-X.Y.Z`)

2. Determine available release branches:
   ```bash
   git branch -r | grep 'release-' | sort -V
   ```

3. Ensure the target release branch is up to date:
   ```bash
   git fetch origin
   git checkout <release-branch>
   git pull origin <release-branch>
   ```

4. Create and push the bugfix branch:
   ```bash
   git checkout -b bugfix/<TICKET-ID>-<short-description>
   git push -u origin bugfix/<TICKET-ID>-<short-description>
   ```

5. Confirm the branch was created:
   ```bash
   git branch --show-current
   ```

## Branch Naming Rules

- Pattern: `bugfix/AMRIT-<ID>-<slug>`
- Slug: lowercase kebab-case, max 5 words, describes the bug (not the fix)
- Example: `bugfix/AMRIT-456-null-pointer-in-anc-save`

## Notes

- Bugfix branches target `release-X.Y.Z`, NOT `main`.
- If the fix is urgent and needs to go to `main` immediately, use `/new-hotfix` instead.
- If the user has uncommitted changes, warn them and ask whether to stash before switching.
- After branching, remind the user to write a failing test that reproduces the bug before implementing the fix.
