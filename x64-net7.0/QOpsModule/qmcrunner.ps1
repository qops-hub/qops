param(

    [PARAMETER(Mandatory = $true)]$sourcePath,
    [PARAMETER(Mandatory = $true)]$qlikDocsPath,
    [PARAMETER(Mandatory = $true)][AllowEmptyString()]$qlikDocsSubfolder,
    [PARAMETER(Mandatory=$false)][String]$reloadOnly,
    [PARAMETER(Mandatory = $true)]$server,
    [PARAMETER(Mandatory = $true)]$runningJob,
    [PARAMETER(Mandatory = $true)]$runningSourceJob,
    [PARAMETER(Mandatory = $false)][switch]$exitOnWarning
)

try{
    Import-Module QlikView-CLI -ErrorAction Stop
}catch{
    [Net.ServicePointManager]::SecurityProtocol = [Net.ServicePointManager]::SecurityProtocol -bor [Net.SecurityProtocolType]::Tls12;
    Install-Module "QlikView-CLI"
}


function Copy-DirectoryWithoutSystemFiles
{
    param (
        [parameter(Mandatory = $true)] [string] $source,
        [parameter(Mandatory = $true)] [string] $destination        
    )

    if( -not (Test-Path -Path $destination -PathType Container)){
        Write-Host ("Creating folder: "+$destination)
        new-item $destination -ItemType Directory | Out-Null;
    }
   
    Get-ChildItem -Path $source -Recurse -Force |
        Where-Object { $_.psIsContainer  } | 
        Where-Object { $name=($_.FullName.Replace($source, '')).ToLower(); return -not (($name.Contains( "-prj") )-or ($name.Contains( "-reduce") ) -or ($name.Contains( "_variables") )-or ($name.Contains( ".git") ))} |
        ForEach-Object { $_.FullName.Replace($source,  $destination )} |
        ForEach-Object { if(-not (Test-Path -PathType Container $_)){  $null = New-Item -ItemType Container -Path $_ } } | Out-Null;

    Get-ChildItem -Path $source -Recurse -Force |
        Where-Object { -not $_.psIsContainer } |
        Where-Object { $name=($_.Directory.FullName.Replace($source, '')).ToLower(); return -not (($name.EndsWith(".qvw")) -or ($name.Contains( "-prj") )-or ($name.Contains( "-reduce") ) -or ($name.Contains( "_variables") )-or ($name.Contains( ".git") ))} |
        Where-Object {-not (Test-Path -PathType Leaf  (($_.FullName.Replace($source, $destination))))} |
        Copy-Item  -Force -Destination { $_.FullName.Replace($source, $destination) }|Out-Null
}

"Started at "+[DateTime]::Now

$uri = ([String]$server).TrimEnd("/") + "/QMS/Service"
$uriParts=$server.TrimStart("http://").TrimStart("https://").Split(':').TrimEnd('/');

$qlikDocsPath = $qlikDocsPath.Replace("\", "/").TrimEnd("/") + "/"
$qlikDocsSubfolder = $qlikDocsSubfolder.Replace("\", "/").TrimStart("/").TrimEnd("/") + "/"
if ($qlikDocsSubfolder -eq "/") {
    $qlikDocsSubfolder = "";
}
if ($runningJob -notin @("Skip", "StopAndReplace", "WaitAndReplace")) {
    $runningJob = 'Skip';
}
if ($runningSourceJob -notin @("Abort", "Wait")) {
    $runningSourceJob = 'Wait';
}


$initialLocation = Get-Location;

try{

    try {
        $Connection=Connect-QlikView -Hostname $uriParts[0] -Port $uriParts[1] -Version IQMS2
        if(-not $PSDefaultParameterValues.ContainsKey('*-Qv*:Connection')){
            $PSDefaultParameterValues.Add('*-Qv*:Connection', $connection)
        }
    }
    catch {
        "Cannot connect to server " + $uri;
        exit 1;
    }

    Set-Location -Path $sourcePath;
    $folderPath=$qlikDocsPath + $qlikDocsSubfolder ;
    $includeList=New-Object -TypeName System.Collections.Generic.List[string];
    if(($reloadOnly -eq $null) -or ($reloadOnly.Length -eq 0)){
        $includeList.Add("*")
    }else {
        $lists=$reloadOnly.Split(";");
        $includeList.AddRange($lists);
    }


    "Cleaning old Qops tasks..."
    $tasks = ([Array]([Array](Get-QVTaskStatuses  -Scope All -Filter @{})) | foreach { Get-QVDocumentTask -Documenttaskid $_.TaskID  -Scope All} )

    foreach($task in $tasks){
        if(-not  $task.Document.Name.EndsWith("_[Qops].qvw") ){
            continue;
        }
        $taskStatus =  Get-QVTaskStatus  -Scope All -Taskid $task.ID
        if ($taskStatus.General.Status -ne "Waiting") {
            "Cannot delete task "+$task.Document.Name+" . It is not in 'Waiting' state"
            continue;
        }
        "Deleting task: "+$task.Document.Name
        $res=Remove-QVTask  -Taskid $task.ID
    }


    "Copying "+$sourcePath+ " into "+$folderPath;
    Copy-DirectoryWithoutSystemFiles -source $sourcePath -destination ($folderPath );
 
    $qvsId = (Get-QVServices  -Servicetypes QlikViewServer).ID

    $docs = Get-QVUserDocuments  -qvsID $qvsId
    [Array]$files = [Array] ((Get-ChildItem  -Path "." -Recurse -Filter "*.qvw" ) | foreach {($_.FullName -replace [regex]::Escape((Get-Location).Path)).TrimStart("\")} );
    $filesList = @();


    foreach ($file in $files) {        

        $fullname = $qlikDocsPath + $qlikDocsSubfolder + $file;
        $replacedName = $file.Replace(".qvw", "_[Qops].qvw");
        $reloadFullName = $qlikDocsPath + $qlikDocsSubfolder + $replacedName;

        foreach($mask in $includeList){
            if($file -like $mask){
                $filesList += @{Name = $file; ReloadName = $replacedName; FullName = $fullname; ReloadFullName = $reloadFullName };
                break;
            }
        }

}
""
""
"Moving documents..."

$i = 0;
$tasks = ([Array]([Array](GetTasks -URI $uri)) | foreach { GetDocumentTask -URI $uri -GUID $_.ID } )
$QDSID = $tasks[0].QDSID;
"QDSID: "+ $QDSID

foreach ($file in $filesList) {
    $outPrefix = '[' + (++$i) + '/' + $filesList.Length + '] '; 
    $fileNameRelative = ($qlikDocsSubfolder + $file.ReloadName)

    $doc = ([ARRAy]($docs | where { ($_.RelativePath + "\" + $_.Name).TrimStart("\") -eq ($fileNameRelative) }))
    $doc
    if ($doc.Count -eq 0) {
        Copy-Item -Path $file.Name -Destination ($file.ReloadFullName) -Force  | out-null;
        $outPrefix + "Document moved " + $file.Name + " -> " + $file.ReloadFullName;
        continue;
    }
    ""
    ""
    "Moving documents..."

    $i = 0;
    $tasks = ([Array]([Array](Get-QVTaskStatuses  -Scope All -Filter @{})) | foreach { Get-QVDocumentTask -Documenttaskid $_.TaskID  -Scope All} )
    $QDSID = $tasks[0].QDSID;
    "QDSID: "+ $QDSID

    foreach ($file in $filesList) {
        $outPrefix = '[' + (++$i) + '/' + $filesList.Length + '] '; 
        $fileNameRelative = ($qlikDocsSubfolder + $file.ReloadName)

        $doc = ([ARRAy]($docs | where { ($_.RelativePath + "\" + $_.Name).TrimStart("\") -eq ($fileNameRelative) }))
        $doc
        if ($doc.Count -eq 0) {
        
            $outPrefix + "Document is moving: " + $file.Name + " -> " + $file.ReloadFullName;
            Copy-Item -Path $file.Name -Destination ($file.ReloadFullName) -Force  | out-null;
            $outPrefix + "Document moved " + $file.Name + " -> " + $file.ReloadFullName;
            continue;
        }
        $outPrefix + "Document already exists"
        $doc = $doc[0];
    
    
        $task = [Array]($tasks | where { $_.Document.ID -eq $doc.ID })
        if ($task.Count -eq 0) {
            $outPrefix + "No task for " + $fileNameRelative
            Copy-Item -Path $file.Name -Destination ($file.ReloadFullName) -Force | Out-Null;
            $outPrefix + "Document moved " + $file.Name + " -> " + $file.ReloadFullName;
            continue;
        }

        [String]$taskID = $task[0].ID;
        $taskStatus =  Get-QVTaskStatus  -Scope All -Taskid $taskID
        if ($taskStatus.General.Status -ne "Running") {
    
            $outPrefix + "Task is not running, replacing document";  
            Copy-Item -Path $file.Name -Destination ($file.ReloadFullName) -Force | Out-Null;
            $outPrefix + "Document moved " + $file.Name + " -> " + $file.ReloadFullName;
            continue;
        }

        if ($runningJob -eq "Skip") {
            $file.Skip = $true
            $outPrefix + "Task is running, document skipped";
            continue;
        }
        if ($runningJob -eq "StopAndReplace") {
            $outPrefix + "Task is running, aborting...";
            $pre = $taskStatus.Extended.FinishedTime;
        
            Stop-QVTask -Taskid $taskID  | Out-Null

            $status = "";
            while ($true) {
                Start-Sleep -s 1;
                $status = Get-QVTaskStatus  -Scope All -Taskid $taskID
                if ($pre -ne $status.Extended.FinishedTime) {
                    break;
                }

            }

            Copy-Item -Path $file.Name -Destination ($file.ReloadFullName) -Force | Out-Null;
            $outPrefix + "Document moved " + $file.Name + " -> " + $file.ReloadFullName;
            continue;
        }
        if ($runningJob -eq "WaitAndReplace") {
            $outPrefix + "Task is running, waiting until finish";
            $pre = $taskStatus.Extended.FinishedTime;
  
            $status = "";
            while ($true) {
                Start-Sleep -s 1;
                $status = Get-QVTaskStatus  -Scope All -Taskid $taskID
                if ($pre -ne $status.Extended.FinishedTime) {
                    break;
                }

            }
            Copy-Item -Path $file.Name -Destination ($file.ReloadFullName) -Force | Out-Null;
            $outPrefix + "Document moved " + $file.Name + " -> " + $file.ReloadFullName;
            continue;
        }
    }


    Clear-QVQVSCache  -Objects UserDocumentList | Out-Null
    ""

    "Checking if tasks are created..."
    $taskIdList = @();
    $i = 0;
    $docs =  Get-QVUserDocuments  -qvsID $qvsId
    $docs | Where-Object {$_.RelativePath -like ($qlikDocsSubfolder + "*")}| foreach {($_.RelativePath + "\" + $_.Name).TrimStart("\") } 
    $maxDocumentReread=10;
    foreach ($file in $filesList) {
        $i++;
        $outPrefix = '[' + $i + '/' + $filesList.Length + '] '; 
        $fileNameRelative = ($qlikDocsSubfolder + $file.ReloadName).Replace("/", "\")
        for($currentRereadCount=0; $currentRereadCount -le $maxDocumentReread; $currentRereadCount++){
            $doc = ([ARRAy]($docs | where { ($_.RelativePath + "\" + $_.Name).TrimStart("\") -eq $fileNameRelative }))
            if ($doc.Count -gt 0) {
                break;
            }
            Write-Host ($outPrefix + "Cant find document: " + $fileNameRelative) -ForegroundColor Red;
            Write-Host("Re-reading QlikServer documents");

            Start-Sleep -s 60;
            $docs =  Get-QVUserDocuments  -qvsID $qvsId
            $docs | Where-Object {$_.RelativePath -like ($qlikDocsSubfolder + "*")}| foreach {($_.RelativePath + "\" + $_.Name).TrimStart("\") } 

        }
        if($doc.Count -le 0){
            if($exitOnWarning){
                Set-Location $initialLocation;
                exit 2;
            }
            Write-Host ("[WARNING]"+$outPrefix + "Cant find document: " + $fileNameRelative) -ForegroundColor Yellow;
            continue;
        }
        $doc = $doc[0];
    
        $folder = (Get-QVDocumentFolder -Id $doc.FolderID -Scope All)[0].General.Path
        $taskName =   ($folder + '/'+$fileNameRelative).TrimStart('/').Replace("'", '').Replace('"', '').Replace("\", "/").Replace("//", '/');
        $taskID = (New-Guid).Guid;

        Write-Host ($outPrefix + "Creating task "+$taskName + " for document with id "+$doc.ID+". Task id is "+$taskID);
        $task=New-QVDocumentTask -Scope "General,Reload,Triggering"   -ID $taskID -QDSID $QDSID -General @{TaskName=$taskName; Enabled=$true; TaskWizardTrack="None"} -Document $doc -Reload @{Mode='Full'; SectionAccessMode='UseQDSAccount'} -Triggering (New-QVTaskTriggering -Executionattempts 1 -Executiontimeout 360 -Triggers @([QlikView_CLI.QMSAPI.ExternalEventTrigger]@{Enabled=$true; ID=(new-guid).Guid; Type='ExternalEventTrigger'; Password="" }) ) 
        Save-QVDocumentTask -Documenttask $task
    
        $taskIdList += ( @{ ID = $taskID; Name = $taskName; FileName = $file.Name; FullName = $file.FullName; ReloadFullName = $file.ReloadFullName; ReloadName = $file.ReloadName });
    }

 
    Clear-QVQVSCache  -Objects UserDocumentList | Out-Null
    Start-sleep -s 10
    "Running tasks..."

    $finished=$true;
    $i = 0;
    foreach ($id in $taskIdList) {
        $i++;
        $fileNameRelative = ($qlikDocsSubfolder + $id.FileName).Replace("/", "\")
        $doc = ([ARRAy]($docs | where { ($_.RelativePath + "\" + $_.Name).TrimStart("\") -eq $fileNameRelative }))
        $outPrefix= "[" + $i + '/' + $taskIdList.Count + ']';
        $outPrefix+" Started at "+[DateTime]::Now;

        if ($doc.Count -gt 0) {
            $doc = $doc[0];
            $task = [Array]($tasks | where { $_.Document.ID -eq $doc.ID })
            if ($task.Count -gt 0) {
                [String]$taskID = $task[0].ID;
                $preStatus = Get-QVTaskStatus  -Scope All -Taskid $taskID

                $status = $preStatus
                if ($status.General.Status -eq "Running") {
                    if ($runningSourceJob -eq "Wait") {
                        "Waiting for source task to finish: " + $fileNameRelative;
                        while ($true) {
                            Start-Sleep -s 1;
                            $status = Get-QVTaskStatus  -Scope All -Taskid $taskID
                            if ($preStatus.Extended.FinishedTime -ne $status.Extended.FinishedTime) {
                                break;
                            }

                        }
                    }
                    else {
                        Stop-QVTask  -Taskid $taskID | Out-Null
                        $status = "";
                        while ($true) {
                            Start-Sleep -s 1;
                            $status = Get-QVTaskStatus  -Scope All -Taskid $taskID
                            if ($preStatus.Extended.FinishedTime -ne $status.Extended.FinishedTime) {
                                break;
                            }

                        }
                    }
                }
            }
        }#  if($doc.Count -gt 0){

   
        $preStatus = Get-QVTaskStatus  -Scope All -Taskid $taskID
        $outPrefix +[DateTime]::Now + " Current task:  Status: $($preStatus.General.Status). Finished time: $($preStatus.Extended.FinishedTime)";

        Start-QVTask  -Taskid $id.ID | Out-Null

        $outPrefix +[DateTime]::Now + ' Running ' + $id.Name+" . Id: "+$id.ID;
        Start-Sleep -s 60;

        $status = "";
        while ($true) {
            Start-Sleep -s 15;
            $status = Get-QVTaskStatus  -Scope All -Taskid $taskID
            if (($preStatus.Extended.FinishedTime -ne $status.Extended.FinishedTime) -or (($status.General.Status -eq "Waiting") -and ($status.Extended.LastLogMessages -ne $preStatus.Extended.LastLogMessages)  )) {
                $outPrefix +[DateTime]::Now + ' Task is finishing ' + $id.Name+" . Id: "+$id.ID+". Status: $($status.General.Status)";
                break;
            }

        }
        $outPrefix +[DateTime]::Now + ' Task finished ' + $id.Name+" . Id: "+$id.ID;
        $status.Extended.LastLogMessages;
        ""
        if ($status.General.Status -ne "Waiting" -or ($preStatus.Extended.FinishedTime -eq $status.Extended.FinishedTime)) {
            $outPrefix +[DateTime]::Now + " Status: $($status.General.Status). Finished time: $($status.Extended.FinishedTime)";
            "Task is failed: " + $id.Name;
            $status.General.OuterXml;
            $status.Extended.OuterXml;

            $finished=$false;
            break;
        }
 
        $newFilesize = 0;
        if (Test-Path -Path $id.ReloadFullName.Replace("\", "/") -PathType Leaf ) {
            $newFilesize = (Get-Item -Path $id.ReloadFullName.Replace("\", "/")).Length
        }
        if (Test-Path -Path $id.FullName.Replace("\", "/") -PathType Leaf ) {
            if ($filesize -lt $newFilesize) {
                "Removing file: " + $id.FullName.Replace("\", "/");
                Remove-Item -Path $id.FullName.Replace("\", "/")  -Force;
            }
            else {
                "File wasnt renamed because its size not changed after reload: " + $id.FullName
                continue;
            }
        }
        Rename-Item -LiteralPath $id.ReloadFullName.Replace("\", "/") -NewName $id.FullName.Replace("\", "/") -Force;

    }
    "Reloading finished"

}
catch{
    "Error happened:"
    $_
}
finally{

    "Removing tasks..."
    foreach ($id in $taskIdList) {
        $res=Remove-QVTask  -Taskid $id.ID
    }
    "Tasks removed..."
    Set-Location $initialLocation;

    if($finished){
        exit 0;
    }
    exit 3;
} 