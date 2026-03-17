[CmdletBinding(PositionalBinding = $false)]
param(
    [Parameter(Position = 0)]
	[string] $rootPath = "C:\",
    [double] $cutoffMinutes = $null,
	[string] $mergedRs = "$PSScriptRoot\rntxfindfiles.txt",
	[string] $archiveRs = $null,
	[string] $excluded = "$PSScriptRoot\excluded.txt",
	[switch] $feedback,
    [int]$StartR = 0,
    [int]$EndR = 100,	
    [string] $fileName = "*",
    [string] $extension = $null
)
if (Test-Path $mergedRs) { Remove-Item -Path $mergedRs -Force }

$cutoff = $null
$sstart = $null
$fileString = $null
$extensionString = $null

if ($fileName -ne "*" -and $extension) {
    $exec = 1
    $extensionString = "*$extension"
    $fileString = "*$fileName*"
} elseif ($fileName -ne "*") {
    $exec = 2
    $fileString = "*$fileName*"
} elseif ($extension) {
    $exec = 3
	$parts = $extension -split '\.'
    $dotCount = $parts.Count - 1
    if ($dotCount -gt 1) {
		$lastExt = $parts[-1]
		$extensionString = "*.$lastExt"
		$fileString = $extension
    } else {
        $extensionString = "*$extension"
    }
} else {
    Write-Host "Invalid arguments. exit"
   exit 1
}


if ($cutoffMinutes -and $cutoffMinutes -gt 0) {
    $cutoff = (Get-Date).AddMinutes(-$cutoffMinutes)
	$sstart = Get-Date
    Write-Host "Cutoff time: $cutoff"
}

function IsExcluded($fullPath, $excludePaths) {
    foreach ($ex in $excludePaths) {
        if ($fullPath.StartsWith($ex, [System.StringComparison]::InvariantCultureIgnoreCase)) {
            return $true
        }
    }
    return $false
}

$defaultExcludes = @(
    "$rootPath\Windows",
    "$rootPath\Program Files",
    "$rootPath\Program Files (x86)"
)

$excludedFile = $excluded

# Check if excluded file exists
if (Test-Path $excludedFile) {
    $excludePaths = Get-Content $excludedFile | ForEach-Object { $_.TrimEnd('\') }
    # Add the default Windows path to the list
    $excludePaths += $defaultExcludes
} else {
    # If file doesn't exist, just use the default
    $excludePaths = $defaultExcludes
}



$topDirs = Get-ChildItem -Path $rootPath -Directory | ForEach-Object { $_.FullName }
$totalTopDirs = $topDirs.Count

$tag = ""
if ($feedback) { $tag = "RESULT:" }
switch ($exec) {
	1 {
		try {
			Get-ChildItem -Path $rootPath -Recurse -Force -File -Filter $extensionString -ErrorAction SilentlyContinue |
			Where-Object {
				
				-not (IsExcluded $_.FullName $excludePaths) -and
				(
					($_.Name -ilike $fileString) -and
					($null -eq $cutoff -or ($_.LastWriteTime -ge $cutoff -or $_.CreationTime -ge $cutoff))
				) -and
				-not $_.PSIsContainer -and
				($_.Attributes -band [System.IO.FileAttributes]::ReparsePoint) -eq 0
			}|
			ForEach-Object {
				$file = $_
				Write-Host "$tag$($file.FullName)"
				
				"$($file.LastWriteTime.ToString("o")),$($file.FullName)" | Out-File -FilePath $mergedRs -Append -Encoding UTF8
				for ($i = 0; $i -lt $topDirs.Count; $i++) {
					if ($file.FullName -like "$($topDirs[$i])\*") {
						$progress = [math]::Round((($i + 1) / $totalTopDirs) * 100)
						Write-Host "Progress: $progress% (top dir: $($topDirs[$i]))"
						$topDirs = $topDirs | Where-Object { $_ -ne $topDirs[$i] }
						break
					}
				}
			}
		}
		catch {
			Write-Host "An error occurred: $_"
		}
	}

	2 {

		try {
			Get-ChildItem -Path $rootPath -Recurse -Force -ErrorAction SilentlyContinue |
			Where-Object {
				
				-not (IsExcluded $_.FullName $excludePaths) -and
				(
					($_.Name -ilike $fileString) -and
					($null -eq $cutoff -or ($_.LastWriteTime -ge $cutoff -or $_.CreationTime -ge $cutoff))
				) -and
				-not $_.PSIsContainer -and
				($_.Attributes -band [System.IO.FileAttributes]::ReparsePoint) -eq 0
			}|
			ForEach-Object {
				$file = $_
				Write-Host "$tag$($file.FullName)"
				"$($file.LastWriteTime.ToString("o")),$($file.FullName)" | Out-File -FilePath $mergedRs -Append -Encoding UTF8

				for ($i = 0; $i -lt $topDirs.Count; $i++) {
					if ($file.FullName -like "$($topDirs[$i])\*") {
						$progress = [math]::Round((($i + 1) / $totalTopDirs) * 100)
						Write-Host "Progress: $progress% (top dir: $($topDirs[$i]))"
						$topDirs = $topDirs | Where-Object { $_ -ne $topDirs[$i] }
						break
					}
				}
			}
		}
		catch {
			Write-Host "An error occurred: $_"
		}
	}

	3 {
		try {
			Get-ChildItem -Path $rootPath -Recurse -Force -File -Filter $extensionString -ErrorAction SilentlyContinue |
			Where-Object {
				#$_.FullName -notmatch '\\Windows($|\\)' -and
				-not (IsExcluded $_.FullName $excludePaths) -and
				(
					($fileString -eq $null -or $_.Name.EndsWith($fileString)) -and
					($null -eq $cutoff -or ($_.LastWriteTime -ge $cutoff -or $_.CreationTime -ge $cutoff))
				) -and
				-not $_.PSIsContainer -and
				($_.Attributes -band [System.IO.FileAttributes]::ReparsesPoint) -eq 0
			}|
			ForEach-Object {
				$file = $_
				Write-Host "$tag$($file.FullName)"

				"$($file.LastWriteTime.ToString("o")),$($file.FullName)" | Out-File -FilePath $mergedRs -Append -Encoding UTF8
				for ($i = 0; $i -lt $topDirs.Count; $i++) {
					if ($file.FullName -like "$($topDirs[$i])\*") {
						$progress = [math]::Round((($i + 1) / $totalTopDirs) * 100)
						Write-Host "Progress: $progress% (top dir: $($topDirs[$i]))"
						$topDirs = $topDirs | Where-Object { $_ -ne $topDirs[$i] }
						break
					}
				}
			}
		}
		catch {
			Write-Host "An error occurred: $_"
		}					
	}
}

if ($archiveRs -and (Test-Path $archiveRs)) { Remove-Item -Path $archiveRs -Force }

if (Test-Path $mergedRs -PathType Leaf) {
	if ($archiveRs) {
		
		try {
			# only the part after the first comma
			Get-Content $mergedRs | ForEach-Object {
				$idx = $_.IndexOf(',')
				if ($idx -ge 0) {
					$_.Substring($idx + 1)
				}
			} | Add-Content -Path $archiveRs
		} catch {
			Write-Warning "Failed to archive $partRs : $_"
		}
	}
	Write-Host "Merge complete: $mergedRs"
}