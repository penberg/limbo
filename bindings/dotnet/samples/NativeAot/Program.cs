using Turso.Samples;

var directory = Path.Combine(Path.GetTempPath(), $"turso-nativeaot-{Guid.NewGuid():N}");
Directory.CreateDirectory(directory);

try
{
    var result = ReplicaPackageSmoke.Run(Path.Combine(directory, "replica.db"));
    Console.WriteLine($"Rows: {result.Rows}");
    Console.WriteLine($"Stats revision: {result.Revision}");
    Console.WriteLine("Checkpoint: complete");
    Console.WriteLine("Disposed: complete");
}
finally
{
    Directory.Delete(directory, recursive: true);
}
