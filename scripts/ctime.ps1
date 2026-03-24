[CmdletBinding(PositionalBinding = $false)]
param(
    [Parameter(Position = 0)]
    [string] $rootPath = "C:\",
    [double] $cutoffMinutes = 15,
	[string] $excluded = "$PSScriptRoot\excluded.txt",
	[switch] $feedback,
	[switch] $progress,
	[int] $StartR = 0,
	[int] $EndR = 100
)
$PSStyle.OutputRendering = 'Ansi'

$epochStart = [datetimeoffset]'1970-01-01T00:00:00Z'
$unixEpochTicks = 621355968000000000L
# $unixEpochTicks = [DateTime]::UnixEpoch.Ticks
# if (-not $unixEpochTicks) { throw "unixEpochTicks not set" }

$cutoff = (Get-Date).AddMinutes(-$cutoffMinutes)
$now = Get-Date
Write-Host "Cutoff time: $cutoff"

# $AppRoot = Split-Path -Parent $PSScriptRoot
# $Bin = Join-Path $AppRoot "bin"
# $env:PATH = "$Bin;$env:PATH"


$excludedPaths = if (Test-Path $excluded) {
    Get-Content $excluded | ForEach-Object { $_.TrimEnd('\') }
} else {
    @(Join-Path $rootPath "Windows")
}

$exRegex = ($excludedPaths | ForEach-Object { [Regex]::Escape($_) }) -join "|"

$dbPath = $mergedRs

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


$allRows = @()

$tag = ""
if ($feedback) { $tag = "RESULT:" }
#get all files in root
Get-ChildItem -Path $rootPath -File -Force -ErrorAction SilentlyContinue |
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
		if ($file.FullName -match "^($exRegex)") {
			continue
		}

		# -and 
		# ($file.Attributes -band [System.IO.FileAttributes]::ReparsePoint) -eq 0
		# -or ($file.LastWriteTime -ge $cutoff -and $file.LastWriteTime -le $now) 
		
		if ( -not $skip -and
			  ($file.CreationTime -ge $cutoff -and $file.CreationTime -le $now) ) {

			if ($feedback) { Write-Host "$tag$($file.FullName)" }

			try {

				# try {
					## $acl = Get-Acl $file -ErrorAction Stop
					## $parts = $acl.Owner.Split('\')
					# $acl = [System.IO.File]::GetAccessControl($file.FullName)
					# $owner = $acl.GetOwner([System.Security.Principal.NTAccount]).Value
					# $parts = $owner.Split('\')
					# $domain = $parts[0]
					# $user   = $parts[1]
				# } catch {
					# $domain = $null
					# $user   = $null
				# }

				# $isSymlink = if (
					# ($file.Attributes -band [IO.FileAttributes]::ReparsePoint) -or
					# ($file.LinkType -eq 'SymbolicLink')
				# ) { 'y' } else { $null }


				$t_dtOffset = [DateTimeOffset]$file.LastWriteTimeUtc
				$ticksDiff = $t_dtOffset.UtcDateTime.Ticks - $unixEpochTicks
				$t_epoch = [int64]($ticksDiff / 10)

				$c_dtOffset = [DateTimeOffset]$file.CreationTimeUtc
				$ticksDiff = $c_dtOffset.UtcDateTime.Ticks - $unixEpochTicks
				$c_epoch = [int64]($ticksDiff / 10)
				# $t_secs = ([DateTimeOffset]$file.LastWriteTimeUtc).ToUnixTimeSeconds()
				# $t_micros = ($file.LastWriteTimeUtc.ToString("o").Split('.')[1]).Substring(0,6)
				# $t_epoch = "$t_secs.$t_micros"
				
				# $c_secs = ([DateTimeOffset]$file.CreationTimeUtc).ToUnixTimeSeconds()
				# $c_micros = ($file.CreationTimeUtc.ToString("o").Split('.')[1]).Substring(0,6)
				# $c_epoch = "$c_secs.$c_micros"

				$a_dtOffset = [datetimeoffset]$file.LastAccessTime
				$a_epoch = ([DateTimeOffset]$file.LastAccessTimeUtc).ToUnixTimeSeconds()

				$row = [PSCustomObject]@{
					timestamp    = $t_epoch
					accesstime   = $a_epoch
					creationtime = $c_epoch
					inode        = 777
					hardlink    = 1
					filesize     = $file.Length
					symlink      = "None"  # $isSymlink
					owner        = "None"  # $user
					domain       = "None"  # $domain
					mode         = $file.Mode
					filename     = $file.FullName
				}
				$allRows += $row
			} catch {
				# Write-Warning "Failed to process file: $($_.Exception.Message)"
				continue
				# ignore file errors rare
			}
		}
	}



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
				# -and ($file.Attributes -band [System.IO.FileAttributes]::ReparsePoint) -eq 0 -and
				# ( ($file.LastWriteTime -ge $cutoff -and $file.LastWriteTime -le $now) -or
				if (-not $skip -and
					  ($file.CreationTime -ge $cutoff -and $file.CreationTime -le $now) ) {

					if ($feedback) { Write-Host "$tag$($file.FullName)" }

					try {

						# try {
							## $acl = Get-Acl $file -ErrorAction Stop
							## $parts = $acl.Owner.Split('\')
							# $acl = [System.IO.File]::GetAccessControl($file.FullName)
							# $owner = $acl.GetOwner([System.Security.Principal.NTAccount]).Value
							# $parts = $owner.Split('\')
							# $domain = $parts[0]
							# $user   = $parts[1]
						# } catch {
							# $domain = $null
							# $user   = $null
						# }

						# $isSymlink = if (
							# ($file.Attributes -band [IO.FileAttributes]::ReparsePoint) -or
							# ($file.LinkType -eq 'SymbolicLink')
						# ) { 'y' } else { $null }


						$t_dtOffset = [DateTimeOffset]$file.LastWriteTimeUtc
						$ticksDiff = $t_dtOffset.UtcDateTime.Ticks - $unixEpochTicks
						$t_epoch = [int64]($ticksDiff / 10)

						$c_dtOffset = [DateTimeOffset]$file.CreationTimeUtc
						$ticksDiff = $c_dtOffset.UtcDateTime.Ticks - $unixEpochTicks
						$c_epoch = [int64]($ticksDiff / 10)
						# $t_secs = ([DateTimeOffset]$file.LastWriteTimeUtc).ToUnixTimeSeconds()
						# $t_micros = ($file.LastWriteTimeUtc.ToString("o").Split('.')[1]).Substring(0,6)
						# $t_epoch = "$t_secs.$t_micros"
		
						# rounds
						# $t_dtOffset = [DateTimeOffset]$file.LastWriteTime
						# $t_epoch = ($t_dtOffset.ToUniversalTime() - $epochStart).TotalSeconds

						# for integer
						#$t_epoch = [int64](($t_dtOffset.UtcDateTime.Ticks - $unixEpochTicks) / 10)  

						# $c_secs = ([DateTimeOffset]$file.CreationTimeUtc).ToUnixTimeSeconds()
						# $c_micros = ($file.CreationTimeUtc.ToString("o").Split('.')[1]).Substring(0,6)
						# $c_epoch = "$c_secs.$c_micros"
						
						# rounds
						# $c_dtOffset = [DateTimeOffset]$file.CreationTime
						# $c_epoch = ($c_dtOffset.ToUniversalTime() - $epochStart).TotalSeconds
						
						# for integer
						# $c_epoch = [int64](($c_dtOffset.UtcDateTime.Ticks - $unixEpochTicks) / 10)

						$a_dtOffset = [DateTimeOffset]$file.LastAccessTime
						$a_epoch = ($a_dtOffset.ToUniversalTime() - $epochStart).TotalSeconds
					
						# symlink      = $isSymlink
						$row = [PSCustomObject]@{
							timestamp    = $t_epoch # $file.LastWriteTime.ToString("o")
							accesstime   = $a_epoch
							creationtime = $c_epoch
							inode        = 777
							hardlink    = 1
							filesize     = $file.Length
							symlink      = "None"  # $isSymlink
							owner        = "None"  # $user
							domain       = "None"  # $domain
							mode         = $file.Mode
							filename     = $file.FullName
						}
						$allRows += $row
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

foreach ($row in $allRows) {
    $line = "$($row.timestamp) $($row.accesstime) $($row.creationtime) $($row.inode) $($row.symlink) $($row.hardlink) $($row.filesize) $($row.owner) $($row.domain) $($row.mode) $($row.filename)"
    Write-Host $line
}

Write-Host "Merge complete:"
