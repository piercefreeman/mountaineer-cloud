# mountaineer-cloud

Limited common code to access cloud resources. Since Mountaineer generally follows a cloud ignostic philosophy, this is only intended to use cloud services for things that can't be reliably reproduced with common Linux conventions:

- Email sending
- Redundant filesystem (S3, B2, etc)

We don't intend to build wrappers for each platform-specific APIs. Instead our goal is to provide common abstraction APIs that will let you do the same _thing_ across different hosts.

## AWS

To use our simpler AWS mocks, you'll need to install our `mocks` extra:

```bash
uv add --dev "mountaineer-cloud[mocks]"
```
