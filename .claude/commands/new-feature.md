# /new-feature

Create a new feature branch following the AMRIT branching strategy.

## Steps

1. Ask the user for:
   - The Jira ticket ID (e.g., `AMRIT-123`)
   - A short description (3–5 words, kebab-case)

2. Determine the current release branch:
   ```bash
   git branch -r | grep 'release-' | sort -V | tail -1
   ```
   If no release branch exists, use `main` and warn the user.

3. Ensure the local release branch is up to date:
   ```bash
   git fetch origin
   git checkout <release-branch>
   git pull origin <release-branch>
   ```

4. Create and push the feature branch:
   ```bash
   git checkout -b feature/<TICKET-ID>-<short-description>
   git push -u origin feature/<TICKET-ID>-<short-description>
   ```

5. Confirm the branch was created:
   ```bash
   git branch --show-current
   ```

## Branch Naming Rules

- Pattern: `feature/AMRIT-<ID>-<slug>`
- Slug: lowercase kebab-case, max 5 words
- Example: `feature/AMRIT-123-add-cbac-screening`

## Notes

- Feature branches MUST be cut from the current `release-X.Y.Z` branch, not from `main`.
- If the user has uncommitted changes, warn them and ask whether to stash before switching.
- Never push directly to `main` or `release-X.Y.Z`.