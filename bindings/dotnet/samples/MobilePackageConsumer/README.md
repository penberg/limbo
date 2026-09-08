# Mobile package-consumer validation

These minimal Android and iOS apps consume `Turso.Data.Sqlite.Provider` through its normal NuGet package layering. Their reachable startup path creates an offline embedded replica when needed, opens it, uses the sync connection and statistics APIs, checkpoints, and disposes the native handles.

The `dotnet-publish` workflow builds the Android ARM64 APK and iOS ARM64 simulator app from the packages produced by the same run. It verifies that each application contains the legacy-named `libturso_sdk_kit` native asset and that the asset exports the sync entry points used by the managed code.

CI does not launch either app. Android emulator and iOS simulator execution are intentionally outside this package build/link check.

For a local compile against the current source tree rather than packed artifacts, pass `-p:TursoUseProjectReferences=true`. CI leaves this property unset so it always tests the package dependency chain.
