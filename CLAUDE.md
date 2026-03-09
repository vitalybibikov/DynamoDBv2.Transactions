# Claude Code Rules

## Git Commits
- NEVER add `Co-Authored-By` or any co-author trailers to commits
- All commits must be authored solely under the repository owner's name
- Do not add yourself as a contributor in any form

## Versioning
- Version format: `{aws_major}.{aws_minor}.{aws_patch}.{aws_rev * 100 + lib_rev}`
- First 3 segments match AWSSDK.DynamoDBv2 major.minor.patch
- 4th segment = (AWS SDK 4th segment × 100) + library revision counter
- Reset library revision to 0 when AWS SDK version bumps
- Example: AWS SDK 4.0.14.1, lib rev 2 → Version 4.0.14.102
