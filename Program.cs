using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Google.Apis.Auth.OAuth2;
using Google.Apis.Drive.v3;
using Google.Apis.Services;
using Google.Apis.Util.Store;

public class Program {
    public static string FOLDER_MIME = "application/vnd.google-apps.folder";
    private static FileDataStore LOCAL_STORAGE;
    private static DriveService DRIVE_TGT;

    static async Task Main(string[] args) {
        LOCAL_STORAGE = new FileDataStore("chown");
        DRIVE_TGT = await authorize("yuzhongh@usc.edu");

        string folder = args[0];
        string parentId = args[1];
        string[] files = Directory.GetFiles(folder);
        Console.WriteLine("Total number of files: " + files.Length);

        var options = new ParallelOptions() { MaxDegreeOfParallelism = 4 };
        Parallel.ForEach(files, options, path => {
            var tgtFile = new Google.Apis.Drive.v3.Data.File {
                Name = Path.GetFileName(path),
                Parents = new string[] { parentId }
            };

            using (var stream = new FileStream(path, FileMode.Open, FileAccess.Read)) {
                var request = DRIVE_TGT.Files.Create(tgtFile, stream, MimeMapping.MimeUtility.GetMimeMapping(path));
                request.Fields = "id";
                request.Upload();

                Console.WriteLine($"Upload successful. {path} => File ID: {request.ResponseBody.Id}");
            }
        });

        Console.WriteLine("All Done");
    }

    private static async Task<DriveService> authorize(string userId) {
        var credential = await GoogleWebAuthorizationBroker.AuthorizeAsync(
            GoogleClientSecrets.FromFile("credentials.json").Secrets,
            new[] { DriveService.Scope.Drive },
            userId,
            CancellationToken.None,
            LOCAL_STORAGE
        );

        return new DriveService(new BaseClientService.Initializer {
            HttpClientInitializer = credential,
        });
    }
}
