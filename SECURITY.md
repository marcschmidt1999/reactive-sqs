# Security

## Supported versions

There is no production release yet. Security fixes apply to the latest development branch.

## Report a vulnerability

Do not open a public issue. Use the private security advisory feature of the GitHub repository
once it exists. Before the first release, the maintainer must add a security contact here.

Include the affected version, impact, steps to reproduce, and a possible fix if you have one.
Do not include AWS credentials, receipt handles, queue URLs with account IDs, or customer data.

## Consumer responsibilities

Applications must use least-privilege IAM, configure AWS SDK retries and timeouts, set SQS
encryption and redrive policies, and make handlers idempotent.
