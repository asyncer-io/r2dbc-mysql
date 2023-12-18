# R2DBC-MYSQL Maintenance

## Releases
Releasing a r2dbc-mysql is a streamlined and automated process typically encompassing three essential steps. However, before delving into these steps, it is imperative to acquaint yourself with the [OSSRH Guide](https://central.sonatype.org/publish/publish-guide/).

**Steps**
1. Initiate the release process
 - Run [GitHub Actions RELEASE workflow](https://github.com/asyncer-io/r2dbc-mysql/actions/workflows/cd-release.yml) to kickstart the release process. Upon completion, the workflow will automatically stage all necessary components in the [Staging Repository](https://s01.oss.sonatype.org/).

2. Artifact Verification
 - Confirm the successful upload of artifacts in the [Staging Repository](https://s01.oss.sonatype.org/) and verify their integrity.

3. Finalize the Release
 - Conclude the release process on the [Staging Repository](https://s01.oss.sonatype.org/) and document the [release notes on GitHub](https://github.com/asyncer-io/r2dbc-mysql/releases).