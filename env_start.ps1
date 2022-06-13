# Disable rule to execute Jupyter 
[System.Environment]::SetEnvironmentVariable("JUPYTER_ALLOW_INSECURE_WRITES", 1, [System.EnvironmentVariableTarget]::User)

$old_path = [System.Environment]::GetEnvironmentVariable("PATH")
$base_path  = Get-Location
$python_path = "$base_path\Python\python-3.8.10.amd64\"
$scripts_path = "$base_path\Python\python-3.8.10.amd64\Scripts\"
$new_path = "$old_path$python_path;$scripts_path;"
$env:Path += $new_path

Invoke-Expression "jupyter contrib nbextension install --user"
Invoke-Expression "jupyter nbextensions_configurator enable --user"
Invoke-Expression "jupyter nbextension enable collapsible_headings/main"
Invoke-Expression "jupyter nbextension enable execute_time/ExecuteTime"
Invoke-Expression "jupyter-notebook $base_path"