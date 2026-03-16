/**
 * Lists all files in a Google Drive folder and logs their name and URL.
 *
 * @param {string} folderId The ID of the Drive folder.
 */
function listFilesInFolder(folderId) {
  var folder = DriveApp.getFolderById(folderId);
  var files = folder.getFiles();

  Logger.log('Files in folder "%s":', folder.getName());

  if (!files.hasNext()) {
    Logger.log('No files found.');
    return;
  }

  while (files.hasNext()) {
    var file = files.next();
    Logger.log('%s - %s', file.getName(), file.getUrl());
  }
}

/**
 * Example runner.
 * Replace FOLDER_ID with an actual Drive folder ID before running.
 */
function runListFilesInFolder() {
  var FOLDER_ID = 'PASTE_FOLDER_ID_HERE';
  listFilesInFolder(FOLDER_ID);
}
