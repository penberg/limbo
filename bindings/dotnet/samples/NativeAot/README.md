# Turso NativeAOT sample

This sample publishes a NativeAOT executable that statically links the legacy-named `turso_sdk_kit` library from the RID-specific `Turso.Data.NativeAot.*` package:

```bash
dotnet publish -c Release -r win-x64
```

Set `<TursoUseStaticNativeLibrary>true</TursoUseStaticNativeLibrary>` with `<PublishAot>true</PublishAot>` and reference the matching `Turso.Data.NativeAot.<rid>` package to enable static native assets. Supported runtime identifiers are `win-x64`, `win-arm64`, `linux-x64`, `linux-arm64`, `osx-x64`, and `osx-arm64`.

The executable creates an offline embedded replica when needed, opens it, connects, writes and reads local data, reads sync statistics, checkpoints, and disposes both the connection and database. This makes NativeAOT publish validate the sync exports rather than only the local database exports.

When using an unreleased local package, add the package output folder as a restore source, for example:

```bash
dotnet publish -c Release -r win-x64 -p:RestoreAdditionalProjectSources=..\..\artifacts\nuget-packages
```
