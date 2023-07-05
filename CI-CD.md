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

### Automated PR Testing

   1. Is Python code in PEP8 format?
   2. Do unit tests pass?
   3. Do new jobs have correct tags to allow for CI/CD workflow?
   4. Are new jobs pointing at the correct filepaths?

## Code Review

1. Code review by at least one other (more?)
   1. If minor notes, make edits and recommit.
   2. If major notes, close PR, return to dev loop.
2. Approve Pull Request
3. Merge (`squash?`) to `main` and delete `origin/<name_of_branch>`

### Automated Push to QA

   1. Update/Create/Delete changed dbricks jobs
      1. Jobs in QA should be paused.
      2. Create ADF "scheduler jobs".
   2. Trigger changed dbricks jobs

## QA Review

1. Manually (gross) confirm status of triggered jobs.
2. Manual data validation.
3. Manual user acceptance testing.
4. At least *n* reviewers must approve before next step.

### Automated Delivery & Deployment

1. Automatically push approved `main` to production environment.
