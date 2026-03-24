#Areas find cant go   * included for now ** not needed and excluded
# DumpStack.log.tmp 
# found.000 
# found.001 
# found.002 
# pagefile.sys 
# swapfile.sys 
# $Recycle.Bin
# .Trash-0
# PerfLogs
# System Volume Information **
# Recovery
# ProgramData\Packages
# ProgramData\WindowsHolographicDevices
# Program Files\Windows Defender Advanced Threat Protection\ **
# ProgramData\Microsoft *
# Users\User\AppData\Local\Temp
# Users\User\AppData\Local\Packages
# Program Files\WindowsApps\
# System Volume Information\ **
# find: ‘/mnt/c/Windows/appcompat/appraiser’: Permission denied
# find: ‘/mnt/c/Windows/appcompat/Backup’: Permission denied
# find: ‘/mnt/c/Windows/appcompat/Programs’: Permission denied
# find: ‘/mnt/c/Windows/CSC’: Permission denied
# find: ‘/mnt/c/Windows/LiveKernelReports’: Permission denied
# find: ‘/mnt/c/Windows/Logs/SystemRestore’: Permission denied
# find: ‘/mnt/c/Windows/Microsoft/Windows/Models’: Permission denied
# find: ‘/mnt/c/Windows/ModemLogs’: Permission denied
# find: ‘/mnt/c/Windows/PLA/Reports’: Permission denied
# find: ‘/mnt/c/Windows/PLA/Rules’: Permission denied
# find: ‘/mnt/c/Windows/PLA/Templates’: Permission denied
# find: ‘/mnt/c/Windows/Prefetch’: Permission denied
# find: ‘/mnt/c/Windows/Provisioning/Autopilot’: Permission denied
# find: ‘/mnt/c/Windows/Resources/Themes/aero/VSCache’: Permission denied
# find: ‘/mnt/c/Windows/security/audit’: Permission denied
# find: ‘/mnt/c/Windows/security/cap’: Permission denied
# find: ‘/mnt/c/Windows/ServiceProfiles/LocalService’: Permission denied
# find: ‘/mnt/c/Windows/ServiceProfiles/NetworkService’: Permission denied
# find: ‘/mnt/c/Windows/ServiceState’: Permission denied
# find: ‘/mnt/c/Windows/System32/Com/dmp’: Permission denied
# find: ‘/mnt/c/Windows/System32/config’: Permission denied
# find: ‘/mnt/c/Windows/System32/Configuration’: Permission denied
# find: ‘/mnt/c/Windows/System32/drivers/DriverData’: Permission denied
# find: ‘/mnt/c/Windows/System32/DriverState’: Permission denied
# find: ‘/mnt/c/Windows/System32/ias’: Permission denied
# find: ‘/mnt/c/Windows/System32/LogFiles/Firewall’: Permission denied
# find: ‘/mnt/c/Windows/System32/LogFiles/WMI’: Permission denied
# find: ‘/mnt/c/Windows/System32/MsDtc’: Permission denied
# find: ‘/mnt/c/Windows/System32/networklist’: Permission denied
# find: ‘/mnt/c/Windows/System32/SleepStudy’: Permission denied
# find: ‘/mnt/c/Windows/System32/spool/PRINTERS’: Permission denied
# find: ‘/mnt/c/Windows/System32/spool/SERVERS’: Permission denied
# find: ‘/mnt/c/Windows/System32/sru’: Permission denied
# find: ‘/mnt/c/Windows/System32/Tasks’: Permission denied
# find: ‘/mnt/c/Windows/System32/Tasks_Migrated’: Permission denied
# find: ‘/mnt/c/Windows/System32/wbem/MOF’: Permission denied
# find: ‘/mnt/c/Windows/System32/WDI’: Permission denied
# find: ‘/mnt/c/Windows/System32/WebThreatDefSvc’: Permission denied
# find: ‘/mnt/c/Windows/SystemTemp’: Permission denied
# find: ‘/mnt/c/Windows/SysWOW64/Com/dmp’: Permission denied
# find: ‘/mnt/c/Windows/SysWOW64/config’: Permission denied
# find: ‘/mnt/c/Windows/SysWOW64/Configuration’: Permission denied
# find: ‘/mnt/c/Windows/SysWOW64/Msdtc’: Permission denied
# find: ‘/mnt/c/Windows/SysWOW64/NetworkList’: Permission denied
# find: ‘/mnt/c/Windows/SysWOW64/sru’: Permission denied
# find: ‘/mnt/c/Windows/SysWOW64/Tasks’: Permission denied
# find: ‘/mnt/c/Windows/WUModels’: Permission denied

#Areas powershell cant go
# Get-ChildItem: Access to the path 'C:\Program Files\Windows Defender Advanced Threat Protection\Classification\Configuration' is denied.
# Get-ChildItem: Access to the path 'C:\Windows\CSC' is denied.
# Get-ChildItem: Access to the path 'C:\Windows\System32\config\BFS' is denied.
# Get-ChildItem: Access to the path 'C:\Windows\System32\LogFiles\WMI\RtBackup' is denied.
# Get-ChildItem: Access to the path 'C:\Windows\System32\WebThreatDefSvc' is denied.

[CmdletBinding(PositionalBinding = $false)]
param(
    [Parameter(Position = 0)]
    [double] $cutoffMinutes = 5,
	[string] $userName = $env:USERNAME,
    [string]$cmin = $null,
    [string]$mmin = $null,
	[switch] $feedback
)
$unixEpochTicks = 621355968000000000L
# $epochStart = [datetimeoffset]'1970-01-01T00:00:00Z'

$searchStartTime = Get-Date
$cutoff = $searchStartTime.AddMinutes(-$cutoffMinutes) #.ToUniversalTime()
Write-Host "Cutoff time: $cutoff"


# $CMinFlag = (-not [string]::IsNullOrEmpty($cmin)) -and ($cmin.ToLower() -eq "true")
# $MMinFlag = (-not [string]::IsNullOrEmpty($mmin)) -and ($mmin.ToLower() -eq "true")

# Narrowed
# "C:\ProgramData\Microsoft",
# "C:\ProgramData\Packages",
# "C:\ProgramData\WindowsHolographicDevices",
    #"C:\$Recycle.Bin",
    #"C:\.Trash-0",
    #"C:\PerfLogs",
    #"C:\Recovery",
# Final



$allRows = @()


# $filter = {
    # if ($MMinFlag) {
        # ($_.LastWriteTime -gt $cutoff -and $_.LastWriteTime -le $searchStartTime)
    # }
    # elseif ($CMinFlag) {
        # ($_.CreationTime -gt $cutoff -and $_.CreationTime -le $searchStartTime)
    # }
# }

$filter = {
    ($_.LastWriteTime -gt $cutoff -and $_.LastWriteTime -le $searchStartTime) 
    
}
																			# -or
# ($_.CreationTime -gt $cutoff -and $_.CreationTime -le $searchStartTime)

# note removed creationtime for tout implementation which handles the created files

$dirs = @(
	"C:\ProgramData",
	"C:\Users\$userName\AppData\Local\Temp",
	"C:\Users\$userName\AppData\Local\Packages",
	"C:\Program Files\WindowsApps"
)


#get all files in base of c:
try {
    $rootFiles = Get-ChildItem -Path 'C:\' -File -ErrorAction Stop |
	Where-Object $filter
	
	# Where-Object { 
		# ($_.LastWriteTime -gt $cutoff -and $_.LastWriteTime -le $searchStartTime) -or
		# ($_.CreationTime -gt $cutoff -and $_.CreationTime -le $searchStartTime)
	# } # -and $_.LastWriteTime -le $searchStartTime 
	
} catch {
    Write-Warning "Error accessing C:\ - $($_.Exception.Message)"
    $rootFiles = @()
}


foreach ($file in $rootFiles) {
    try {

        # try {
            # $onr = (Get-Acl $file.FullName).Owner
            # $ownerParts = $onr.Split('\')
            # $domain = if ($ownerParts.Count -gt 1) { $ownerParts[0] } else { $null }
            # $user = if ($ownerParts.Count -gt 1) { $ownerParts[1] } else { $ownerParts[0] }
        # } catch {
            # $onr = "None"
            # $domain = "None"
            # $user = "None"
        # }
		# note integer
		$t_dtOffset = [DateTimeOffset]$file.LastWriteTimeUtc
		$ticksDiff = $t_dtOffset.UtcDateTime.Ticks - $unixEpochTicks
		$t_epoch = [int64]($ticksDiff / 10)

		$c_dtOffset = [DateTimeOffset]$file.CreationTimeUtc
		$ticksDiff = $c_dtOffset.UtcDateTime.Ticks - $unixEpochTicks
		$c_epoch = [int64]($ticksDiff / 10)
		# current
		# $t_iso = $file.LastWriteTime.ToString("o")

		# $t_secs = ([DateTimeOffset]$file.LastWriteTimeUtc).ToUnixTimeSeconds()
		# $t_micros = ($file.LastWriteTimeUtc.ToString("o").Split('.')[1]).Substring(0,6)
		# $t_epoch = "$t_secs.$t_micros"

		# alternative
		# $t_dtOffset = [DateTimeOffset]$file.LastWriteTimeUtc
		# $ticks = $t_dtOffset.Ticks - $unixEpochTicks
		# $secs = [int64]([decimal]$ticks / 10000000)
		# $micros = [int64]([decimal]($ticks % 10000000L) / 10)
		# $t_epoch = "{0}.{1:D6}" -f $secs, $micros

		# integer
		# rounds 
		# $t_epoch = [int64](($c_dtOffset.UtcDateTime.Ticks - $unixEpochTicks) / 10)
		# truncate  - preferred over above for matching mtime_us to not be off by 1 or 2 us
		# $t_epoch = ([DateTimeOffset]$file.LastWriteTimeUtc).Ticks - $unixEpochTicks
		# $t_epoch = [int64]([decimal]$ticks / 10)
						
		# current
		# $c_iso = $file.CreationTime.ToString("o")

		# $c_secs = ([DateTimeOffset]$file.CreationTimeUtc).ToUnixTimeSeconds()
		# $c_micros = ($file.CreationTimeUtc.ToString("o").Split('.')[1]).Substring(0,6)
		# $c_epoch = "$c_secs.$c_micros"

		# alternative
		# $c_dtOffset = [DateTimeOffset]$file.CreationTimeUtc
		# $ticks = $c_dtOffset.Ticks - $unixEpochTicks
		# $secs = [int64]([decimal]$ticks / 10000000)
		# $micros = [int64]([decimal]($ticks % 10000000L) / 10)
		# $c_epoch = "{0}.{1:D6}" -f $secs, $micros
		
		# integer
		# rounds
		# $c_epoch = [int64](($c_dtOffset.UtcDateTime.Ticks - $unixEpochTicks) / 10)
		# truncate
		# $c_epoch = ([DateTimeOffset]$file.CreationTimeUtc).Ticks - $unixEpochTicks
		# $c_epoch = [int64]([decimal]$ticks / 10)
		
		$a_dtOffset = [DateTimeOffset]$file.LastAccessTime
		$a_epoch = ([DateTimeOffset]$file.LastAccessTimeUtc).ToUnixTimeSeconds()
		# $a_epoch = [math]::Round(($a_dtOffset.ToUniversalTime() - $epochStart).TotalSeconds)  # original

        # $isSymlink = if ($file.Attributes -band [IO.FileAttributes]::ReparsePoint) { 'y' } else { $null }

        $row = [PSCustomObject]@{
			timestamp    = $t_epoch
			accesstime   = $a_epoch
			creationtime = $c_epoch
            inode        = 777
            hardlink    = 1
            filesize     = $file.Length
            owner        = "None" # $user
            domain       = "None" # $domain
            mode         = $file.Mode
            filename     = $file.FullName
        }

        $allRows += $row

    } catch {
        #Write-Warning "Error processing file $($file.FullName): $($_.Exception.Message)"
		continue
    }
}

#find the files the find command cant
foreach ($dir in $dirs) {
    #Write-Host "In directory- $dir"

    try {
        # Get files in directory modified or created after cutoff
        $files = Get-ChildItem -Path $dir -File -ErrorAction Stop -Recurse |
		Where-Object $filter
		# Where-Object {
			# ($_.LastWriteTime -gt $cutoff -and $_.LastWriteTime -le $searchStartTime) -or
			# ($_.CreationTime -gt $cutoff -and $_.CreationTime -le $searchStartTime)# -and $_.LastWriteTime -le $searchStartTime # $_.CreationTime -gt $cutoff -or 
		# }
    } catch {
        #Write-Warning "Error accessing dir ${dir}: $($_.Exception.Message)"
        continue
    }

    if (-not $files -or $files.Count -eq 0) {
        #Write-Host "No files found in $dir."
        continue
    }

    foreach ($file in $files) {
        try {

            # try {
                # $onr = (Get-Acl $file.FullName).Owner
                # $ownerParts = $onr.Split('\')
                # $domain = if ($ownerParts.Count -gt 1) { $ownerParts[0] } else { "None" } # $null
                # $user = if ($ownerParts.Count -gt 1) { $ownerParts[1] } else { $ownerParts[0] }
            # } catch {
                ## Write-Warning "Failed to get owner for $($file.FullName): $($_.Exception.Message)"
                # $onr = "None"
                # $domain = "None"
                # $user = "None"
            # }
			# note integer
			$t_dtOffset = [DateTimeOffset]$file.LastWriteTimeUtc
			$ticksDiff = $t_dtOffset.UtcDateTime.Ticks - $unixEpochTicks
			$t_epoch = [int64]($ticksDiff / 10)

			$c_dtOffset = [DateTimeOffset]$file.CreationTimeUtc
			$ticksDiff = $c_dtOffset.UtcDateTime.Ticks - $unixEpochTicks
			$c_epoch = [int64]($ticksDiff / 10)
		
			# $t_secs = ([DateTimeOffset]$file.LastWriteTimeUtc).ToUnixTimeSeconds()
			# $t_micros = ($file.LastWriteTimeUtc.ToString("o").Split('.')[1]).Substring(0,6)
		
			# $c_secs = ([DateTimeOffset]$file.CreationTimeUtc).ToUnixTimeSeconds()
			# $c_micros = ($file.CreationTimeUtc.ToString("o").Split('.')[1]).Substring(0,6)
		
			$a_dtOffset = [DateTimeOffset]$file.LastAccessTime
			$a_epoch = ([DateTimeOffset]$file.LastAccessTimeUtc).ToUnixTimeSeconds()

			$row = [PSCustomObject]@{
				timestamp    = $t_epoch # $file.LastWriteTime.ToString("o")
				accesstime   = $a_epoch
				creationtime = $c_epoch
                inode        = 777
                hardlink    = 1
                filesize     = $file.Length
                owner        = "None" # $user
                domain       = "None" # $domain
                mode         = $file.Mode
                filename     = $file.FullName
            }

            $allRows += $row

        } catch {
            #Write-Warning "Error processing file $($file.FullName): $($_.Exception.Message)"
			continue
        }
    }
}

foreach ($row in $allRows) {
    $line = "$($row.timestamp) $($row.accesstime) $($row.creationtime) $($row.inode) $($row.mode) $($row.hardlink) $($row.filesize) $($row.owner) $($row.domain) $($row.mode) $($row.filename)"
    Write-Host $line
}	
	# mode is -a---  myapp expect 777	
	#$isSymlink = if ($file.Attributes -band [IO.FileAttributes]::ReparsePoint) { 'y' } else { $null }
	#"%T@ %A@ %C@ %i %s %u %g %m %p\n"
	# 	symlink      = $isSymlink