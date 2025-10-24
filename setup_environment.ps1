# PowerShell script to set up the Python virtual environment and install dependencies

$venvPath = ".\.venv"
$pythonExe = "$venvPath\Scripts\python.exe"
$pipExe = "$venvPath\Scripts\pip.exe"
$activateScript = "$venvPath\Scripts\Activate.ps1"

Write-Host "Checking for existing virtual environment..."

# 1. Create the virtual environment if it doesn't exist
if (-not (Test-Path $venvPath)) {
    Write-Host "Virtual environment not found. Creating it now..."
    # Assuming 'py' or 'python' is available globally to create the venv initially
    # If 'py' is not found, this step will fail and manual intervention might be needed
    try {
        py -m venv $venvPath
        if (-not (Test-Path $pythonExe)) {
            Write-Host "Failed to create virtual environment using 'py'. Trying 'python'..."
            python -m venv $venvPath
        }
    } catch {
        Write-Error "Could not create virtual environment. Please ensure 'py' or 'python' is in your PATH or provide its full path."
        exit 1
    }
    Write-Host "Virtual environment created at $venvPath"
} else {
    Write-Host "Virtual environment already exists."
}

# 2. Activate the virtual environment
Write-Host "Activating virtual environment..."
& $activateScript

# 3. Ensure pip is installed and up-to-date within the venv
Write-Host "Ensuring pip is installed and up-to-date..."
if (-not (Test-Path $pipExe)) {
    Write-Host "pip not found in venv. Installing it..."
    & $pythonExe -m ensurepip --upgrade
} else {
    Write-Host "pip found. Upgrading it..."
    & $pythonExe -m pip install --upgrade pip
}

# 4. Install required Python packages
Write-Host "Installing required Python packages..."
& $pythonExe -m pip install psutil google-generativeai playwright pytz

# 5. Install Playwright browsers
Write-Host "Installing Playwright browsers..."
& $pythonExe -m playwright install

Write-Host "`nSetup complete!"
Write-Host "To activate the environment in your current PowerShell session, run:"
Write-Host "  & $activateScript"
Write-Host "Then you can run your Python scripts, e.g.:"
Write-Host "  python contact-extractor.py"
