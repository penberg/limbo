using Foundation;
using Turso.Samples;
using UIKit;

namespace Turso.PackageConsumer.iOS;

[Register("AppDelegate")]
public sealed class AppDelegate : UIApplicationDelegate
{
    public override bool FinishedLaunching(UIApplication application, NSDictionary? launchOptions)
    {
        var directory = Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData);
        var result = ReplicaPackageSmoke.Run(Path.Combine(directory, "replica.db"));
        if (result.Rows != 3)
            throw new InvalidOperationException($"Expected 3 rows, got {result.Rows}.");

        return true;
    }
}
