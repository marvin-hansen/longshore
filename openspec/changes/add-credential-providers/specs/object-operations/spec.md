## MODIFIED Requirements

### Requirement: Presigned URLs
The library SHALL produce a presigned URL for a fetch or a store of one key,
valid for a stated duration, written into a caller-provided buffer without
performing I/O or allocating. It SHALL refuse a URL that would outlive the
temporary credentials it is signed with.

#### Scenario: Presigned fetch
- **WHEN** a presigned fetch URL for an existing key is requested with a one-hour expiry
- **THEN** a plain HTTP GET of that URL with no other credentials returns the object until the hour has passed

#### Scenario: Expiry longer than the credentials
- **WHEN** the credentials in use are temporary and expire before the requested URL expiry
- **THEN** the request for a URL fails with an error that says so
