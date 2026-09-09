using Android.App;
using Android.OS;
using Turso.Samples;

namespace Turso.PackageConsumer.Android;

[Activity(Label = "Turso package consumer", MainLauncher = true)]
public sealed class MainActivity : Activity
{
    protected override void OnCreate(Bundle? savedInstanceState)
    {
        base.OnCreate(savedInstanceState);

        var directory = FilesDir?.AbsolutePath
            ?? throw new InvalidOperationException("Android did not provide an application files directory.");
        var result = ReplicaPackageSmoke.Run(Path.Combine(directory, "replica.db"));
        if (result.Rows != 3)
            throw new InvalidOperationException($"Expected 3 rows, got {result.Rows}.");
    }
}
