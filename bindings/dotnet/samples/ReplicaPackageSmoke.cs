using System.Net;
using Turso;

namespace Turso.Samples;

internal readonly record struct ReplicaPackageSmokeResult(long Rows, string Revision);

internal static class ReplicaPackageSmoke
{
    internal static ReplicaPackageSmokeResult Run(string path)
    {
        using var httpClient = new HttpClient(new UnexpectedHttpHandler());
        var options = new TursoSyncDatabaseOptions(path, new Uri("https://example.invalid"))
        {
            BootstrapIfEmpty = false,
            HttpClient = httpClient,
        };

        if (!File.Exists(path))
        {
            using var created = TursoSyncDatabase.Create(options);
        }

        using var database = TursoSyncDatabase.Open(options);
        long rows;
        using (var connection = database.Connect())
        {
            connection.ExecuteNonQuery("CREATE TABLE IF NOT EXISTS items (value TEXT NOT NULL)");
            connection.ExecuteNonQuery("DELETE FROM items");
            connection.ExecuteNonQuery("INSERT INTO items VALUES ('native'), ('sync'), ('turso')");
            using var command = new TursoCommand(connection, "SELECT COUNT(*) FROM items");
            rows = Convert.ToInt64(command.ExecuteScalar());
        }

        var stats = database.GetStats();
        database.Checkpoint();
        return new ReplicaPackageSmokeResult(rows, stats.Revision);
    }

    private sealed class UnexpectedHttpHandler : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request,
            CancellationToken cancellationToken)
            => throw new InvalidOperationException(
                $"BootstrapIfEmpty=false must not make an HTTP request ({request.Method} {request.RequestUri}).");
    }
}
