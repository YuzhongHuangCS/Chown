using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Google.Apis.Auth.OAuth2;
using Google.Apis.Drive.v3;
using Google.Apis.Services;
using Google.Apis.Util.Store;

public class Program {
    public static string FOLDER_MIME = "application/vnd.google-apps.folder";
    private static FileDataStore LOCAL_STORAGE;
    private static DriveService DRIVE_SRC;
    private static DriveService DRIVE_TGT;

    static async Task Main(string[] args) {
        LOCAL_STORAGE = new FileDataStore("chown");
        DRIVE_SRC = await authorize("willyihe@gmail.com");
        DRIVE_TGT = await authorize("yuzhongh@usc.edu");

        string fileId = args[0];
        var queryRequest = DRIVE_SRC.Files.Get(fileId);
        queryRequest.Fields = "mimeType";
        var queryFie = await queryRequest.ExecuteAsync();
        if (queryFie.MimeType == FOLDER_MIME) {
            Console.WriteLine("Listing Directory: " + fileId);
            var fileList = DRIVE_SRC.Files.List();
            fileList.Q = $"'{fileId}' in parents";
            var fileIds = new List<string>();

            do {
                var listResults = await fileList.ExecuteAsync();
                fileIds.AddRange(listResults.Files.Select(f => f.Id));
                fileList.PageToken = listResults.NextPageToken;
            } while (fileList.PageToken != null) ;

            Console.WriteLine("Total number of files: " + fileIds.Count);
            var options = new ParallelOptions() { MaxDegreeOfParallelism = 4 };
            Parallel.ForEach(fileIds, options, Chown);
        } else {
            Chown(fileId);
        }
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

    private static void Chown(string fileId) {
        var req = DRIVE_SRC.Files.Get(fileId);
        req.Fields = "id,name,mimeType,parents,owners,ownedByMe";

        var srcFile = req.Execute();
        if (srcFile.OwnedByMe == true) {
            if (srcFile.MimeType != FOLDER_MIME) {
                using (var stream = new MemoryStream()) {
                    req.Download(stream);
                    stream.Position = 0;

                    var tgtFile = new Google.Apis.Drive.v3.Data.File {
                        Name = srcFile.Name,
                        Parents = srcFile.Parents
                    };

                    var request = DRIVE_TGT.Files.Create(tgtFile, stream, srcFile.MimeType);
                    request.Fields = "id";
                    request.Upload();
                    
                    Console.WriteLine($"{srcFile.Name}: {srcFile.Id} -> {request.ResponseBody.Id}");
                    DRIVE_SRC.Files.Delete(srcFile.Id).Execute();
                }
            } else {
                Console.WriteLine(fileId + " is a folder");
            }
        } else {
            Console.WriteLine(fileId + " is not owned by me");
        }
    }
}
