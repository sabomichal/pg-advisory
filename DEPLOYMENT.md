# Deployment Instructions

Artifacts are published to Maven Central through the [Central Portal](https://central.sonatype.org/publish/publish-portal-maven/)
using `central-publishing-maven-plugin`, all modules together in one deployment: `pg-advisory-parent`, `pg-advisory`,
`pg-advisory-spring-boot` and `pg-advisory-spring-boot-starter`. The `central` server credentials (Portal user token) and `gpg.passphrase`
must be present in `~/.m2/settings.xml`.

The version is `0-SNAPSHOT` between releases, in the parent `pom.xml` and in the `<parent>` of every module,
so the release version has to be bumped by hand.
Pick the next version after the latest [release](https://github.com/sabomichal/pg-advisory/releases) following
semantic versioning:

* patch (`2.0.1` → `2.0.2`): bug fixes and dependency updates
* minor (`2.0.2` → `2.1.0`): new features, backwards compatible
* major (`2.1.0` → `3.0.0`): breaking changes, e.g. a new Spring Boot major version or a raised minimum Java version

## Release steps

1. Point the README at the new version and commit:
   ```shell
   git commit -am "docs: point README at X.Y.Z"
   ```
2. Set the release version in all poms, commit and create an annotated tag
   (a lightweight tag is not pushed by `--follow-tags`):
   ```shell
   mvn -B versions:set -DnewVersion=X.Y.Z
   git commit -am "release version X.Y.Z"
   git tag -a vX.Y.Z -m "vX.Y.Z"
   ```
3. Deploy. The `sign` profile signs the artifacts, and the deployment is published automatically after validation.
   The profile has to be enabled explicitly, `gpg.passphrase` from `settings.xml` does not activate it.
   On Windows point the plugin to the GnuPG installation holding the key, the `gpg` bundled with Git Bash has an empty keyring:
   ```shell
   mvn -B clean deploy -Psign -Dgpg.executable=C:/bin/GnuPG/bin/gpg.exe
   ```
4. Set the version back to `0-SNAPSHOT`, commit and push the commits together with the tag:
   ```shell
   mvn -B versions:set -DnewVersion=0-SNAPSHOT
   git commit -am "next development version"
   git push --follow-tags origin master
   ```
5. Create a [GitHub release](https://github.com/sabomichal/pg-advisory/releases/new) for the tag `vX.Y.Z` with release notes.
