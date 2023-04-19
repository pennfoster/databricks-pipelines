# CI/CD Flow

## Setup

1. Start a new branch
   1. `git switch -c <name_of_branch>`
2. Set an upstream origin
   1. `git push -u origin <name_of_branch>`

## Development loop

1. Write code and tests
2. Add changes 
   1. `git add <path/to/changes>` (isolated "adds" allow for better commit messages)
3. Commit with descriptive and brief message
   1. `git commit -m "work represented by this commit"`
4. On commit formatting (`black`).
5. Push changes to origin
   1. `git push origin/<name_of_branch>`
6. Repeat...

## Setup orchestration loop

### DBricks Jobs

1. Create and run new job through DBricks UI in Development
2. Copy job as JSON object into new branch of Jobs repo
3. Repeat...

## Pull Requests

1. Pull from `main` branch and merge into development branch to ensure current code
   1. Resolve merge conflicts
2. Submit Pull Request
3. Code review
   1. Notes/rejection: Return to development
4. Approve Pull Request
5. Merge (`squash?`) to `main` and delete `origin/<name_of_branch>`

## Testing

1. (at any step revert changes & return to development loop on failure)
2. Automatic formatting check
3. Automatic unit tests
4. Automatic code coverage test
5. Automatic push updated `main` to QA/UAT enviornment
6. Automatic new/changed data pipeline execution
7. Manual user acceptance testing by stakeholders as needed

## Delivery & Deployment

1. Manual approval of production workflow
2. Automatically push approved `main` to production environment.
