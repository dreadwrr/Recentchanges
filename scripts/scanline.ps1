[CmdletBinding(PositionalBinding = $false)]
param(
    [Parameter(Position = 0)]
    [string] $rootPath = "C:\",
    [double] $cutoffMinutes = 5,
	[string] $mergedRs = "$PSScriptRoot\recent_merged.db",
	[string] $excluded = "$PSScriptRoot\excluded.txt",
	[switch] $feedback,
	[switch] $progress,
	[int] $StartR = 0,
	[int] $EndR = 100
)
$PSStyle.OutputRendering = 'Ansi'
#$epochStart = [datetimeoffset]'1970-01-01T00:00:00Z'
$unixEpochTicks = 621355968000000000L
# $unixEpochTicks = [DateTime]::UnixEpoch.Ticks
# if (-not $unixEpochTicks) { throw "unixEpochTicks not set" }

$cutoff = (Get-Date).AddMinutes(-$cutoffMinutes)
$now = Get-Date
Write-Host "Cutoff time: $cutoff"

$AppRoot = Split-Path -Parent $PSScriptRoot
$Bin = Join-Path $AppRoot "bin"
$env:PATH = "$Bin;$env:PATH"

$ModulePath = Join-Path $AppRoot "modules"
$env:PSModulePath = "$ModulePath;$env:PSModulePath"

# $basePath = [string]$PSScriptRoot
# $dbPath   = Join-Path $basePath "recent_files.db"

#$OutPath = Split-Path $mergedRS
#$excludedFile = Join-Path $OutPath $excluded


$excludedPaths = if (Test-Path $excluded) {
    Get-Content $excluded | ForEach-Object { $_.TrimEnd('\') }
} else {
    @(Join-Path $rootPath "Windows")
}

$exRegex = ($excludedPaths | ForEach-Object { [Regex]::Escape($_) }) -join "|"

$dbPath = $mergedRs

if (Test-Path $dbPath) {
	try {
		Remove-Item -Path $dbPath -Force
		#Write-Host "Deleted: ${partDb}"
	} catch {
		#Write-Warning "Failed to delete ${partDb}: $_"
	}
}
	
Import-Module PSSQLite
$conn = New-SQLiteConnection -DataSource $dbPath #| Out-Null
$conn | Write-Output
Invoke-SqliteQuery -DataSource $dbPath -Query @"
CREATE TABLE IF NOT EXISTS files (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp INTEGER,
    filename TEXT,
    creationtime INTEGER,
    inode TEXT,
    accesstime TEXT,
    checksum TEXT,
    filesize INTEGER,
    symlink TEXT,
    owner TEXT,
    domain TEXT,
    mode TEXT,
    casmod TEXT,
    lastmodified TEXT,
    hardlinks TEXT
);
"@

$topDirs = Get-ChildItem -Path $rootPath -Directory -Force -ErrorAction SilentlyContinue |
    Where-Object {
        $skip = $false
        foreach ($ex in $excludedPaths) {
            if ($_.FullName.StartsWith($ex, [System.StringComparison]::InvariantCultureIgnoreCase)) {
                $skip = $true
                break
            }
        }
        -not $skip -and ($_.Attributes -band [System.IO.FileAttributes]::ReparsePoint) -eq 0
    }

# $topDirs | ForEach-Object { Write-Host $_ }
# exit

$totalDirs = $topDirs.Count
$currentDirIndex = 0

$skippedDirs = @()

$rows = @()
# Invoke-SqliteQuery -Connection $conn -Query "BEGIN TRANSACTION;"

foreach ($dir in $topDirs) {
    $currentDirIndex++

	try {
		Get-ChildItem -Path $dir.FullName -File -Recurse -Force -ErrorAction SilentlyContinue |
			ForEach-Object {
				
				$file = $_
				$skip = $false
				# original exclude block
				# foreach ($ex in $excludedPaths) {
					# if ($file.FullName.StartsWith($ex, [System.StringComparison]::InvariantCultureIgnoreCase)) {
						# $skip = $true
						# break
					# }
				# }
				if ($file.FullName -match "^($exRegex)")
				{
					continue
				}
				if (-not $skip -and ($file.Attributes -band [System.IO.FileAttributes]::ReparsePoint) -eq 0 -and
					( ($file.LastWriteTime -ge $cutoff -and $file.LastWriteTime -le $now) -or
					  ($file.CreationTime -ge $cutoff -and $file.CreationTime -le $now) )) {

					if ($feedback) { Write-Host $file.FullName }

					try {

						try {
							# $acl = Get-Acl $file -ErrorAction Stop
							# $parts = $acl.Owner.Split('\')
							$acl = [System.IO.File]::GetAccessControl($file.FullName)
							$owner = $acl.GetOwner([System.Security.Principal.NTAccount]).Value
							$parts = $owner.Split('\')
							$domain = $parts[0]
							$user   = $parts[1]
						} catch {
							$domain = $null
							$user   = $null
						}

						$isSymlink = if (
							($file.Attributes -band [IO.FileAttributes]::ReparsePoint) -or
							($file.LinkType -eq 'SymbolicLink')
						) { 'y' } else { $null }

						# $deltaTicks = [int64]($file.LastWriteTimeUtc.Ticks - $unixEpochTicks)   # 100ns units
						# $t_epoch = [int64][math]::Truncate(([decimal]$deltaTicks) / 10)   
						
						#$t_epoch = [int64](($file.LastWriteTimeUtc.Ticks - $unixEpochTicks) / 10)
						
						# $deltaTicks = [int64]($file.CreationTimeUtc.Ticks - $unixEpochTicks)   # 100ns units
						# $c_epoch = [int64][math]::Truncate(([decimal]$deltaTicks) / 10)
						
						# $c_epoch = [int64](($file.CreationTimeUtc.Ticks - $unixEpochTicks) / 10)

						$rows += ,@{
							timestamp    = $file.LastWriteTime.ToString("o")
							filename     = $file.FullName
							creationtime = $file.CreationTime.ToString("o")
							inode        = $null
							accesstime   = $file.LastAccessTime.ToString("o")
							checksum     = $null
							filesize     = $file.Length #.ToString()
							symlink      = $isSymlink
							owner		 = $user
							domain       = $domain
							mode         = $file.Mode
							casmod       = $null
							lastmodified = $null
							hardlinks    = $null
						}
				
						# $insertQuery = @"
# INSERT INTO files (timestamp, filename, creationtime, inode, accesstime, checksum, filesize, symlink, owner, domain, mode, casmod, lastmodified, hardlinks)
# VALUES (
    # '$($file.LastWriteTime.ToString("o"))',
    # '$($file.FullName.Replace("'", "''"))',
    # '$($file.CreationTime.ToString("o"))',
    # NULL,
    # '$($file.LastAccessTime.ToString("o"))',
    # NULL,
    # '$($file.Length)',
    # '$isSymlink',
    # '$user',
    # '$domain',
    # '$($file.Mode)',
    # NULL,
    # NULL,
    # NULL
# );
#"@

					# Invoke-SqliteQuery -DataSource $dbPath -Query $query  # original
					# Invoke-SqliteQuery -Connection $conn -Query $insertQuery
				} catch {
					# Write-Warning "Failed to process file: $($_.Exception.Message)"
					continue
					# ignore file errors rare
				}
			}
		}
    $rawPercent = ($currentDirIndex / $totalDirs) * 100
    $progressPercent = [math]::Round($StartR + ($rawPercent / 100 * ($EndR - $StartR)))

	if ($progress) {
		Write-Host "Progress: $progressPercent%"
	}
	} catch {
		#Write-Warning " $($dir.FullName): $_ $($_.Exception.Message)"
		# $skippedDirs += $dir.FullName      some
		continue
	}
}

if ($rows.Count -gt 0) {
    $tx = $conn.BeginTransaction()
    $cmd = $conn.CreateCommand()
    $cmd.Transaction = $tx
    $cmd.CommandText = @"
INSERT INTO files (
    timestamp, filename, creationtime, inode, accesstime, checksum,
    filesize, symlink, owner, domain, mode, casmod, lastmodified, hardlinks
) VALUES (
    @timestamp, @filename, @creationtime, @inode, @accesstime, @checksum,
    @filesize, @symlink, @owner, @domain, @mode, @casmod, @lastmodified, @hardlinks
)
"@

    foreach ($name in @(
        "timestamp","filename","creationtime","inode","accesstime","checksum",
        "filesize","symlink","owner","domain","mode","casmod","lastmodified","hardlinks"
    )) {
        [void]$cmd.Parameters.Add((New-Object System.Data.SQLite.SQLiteParameter("@$name")))
    }

    try {
        foreach ($row in $rows) {
            foreach ($key in $row.Keys) {
                $value = $row[$key]
                $cmd.Parameters["@$key"].Value = if ($null -eq $value) { [DBNull]::Value } else { $value }
            }
            [void]$cmd.ExecuteNonQuery()
        }
        $tx.Commit()
    } catch {
        $tx.Rollback()
        throw
    } finally {
        $cmd.Dispose()
        $tx.Dispose()
    }
}
# Commit 
# Invoke-SqliteQuery -Connection $conn -Query "COMMIT;"

# Write-Host "Skipped directories:"
# $skippedDirs | ForEach-Object { Write-Host $_ }

$conn.Close()
$conn.Dispose()

Write-Host "Merge complete: $dbPath"