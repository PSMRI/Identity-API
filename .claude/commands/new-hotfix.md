# /new-hotfix

Create a hotfix branch from `main` for urgent production fixes.

## Steps

1. Ask the user for:
   - The Jira ticket ID (e.g., `AMRIT-789`)
   - A short description of the production issue (3–5 words, kebab-case)
   - Confirmation that this is a production incident (not a regular bugfix)

2. Ensure `main` is up to date:
   ```bash
   git fetch origin
   git checkout main
   git pull origin main
   ```

3. Create and push the hotfix branch:
   ```bash
   git checkout -b hotfix/<TICKET-ID>-<short-description>
   git push -u origin hotfix/<TICKET-ID>-<short-description>
   ```

4. Confirm the branch was created:
   ```bash
   git branch --show-current
   git log --oneline -3
   ```

## Branch Naming Rules

- Pattern: `hotfix/AMRIT-<ID>-<slug>`
- Slug: lowercase kebab-case, max 5 words
- Example: `hotfix/AMRIT-789-fix-jwt-expiry-crash`

## Post-Merge Reminders

After the hotfix PR merges to `main`, remind the user to:
1. Tag the release: `git tag -a vX.Y.Z+1 -m "Hotfix: <description>"`
2. Back-merge `main` into the current `release-X.Y.Z` branch so the fix is not lost:
   ```bash
   git checkout release-X.Y.Z
   git merge main
   git push origin release-X.Y.Z
   ```

## Notes

- Hotfix branches MUST be cut from `main`, not from a release branch.
- Use this only for critical production issues. For non-urgent fixes use `/new-bugfix`.
- If the user has uncommitted changes, warn them and ask whether to stash before switching.
