Set WshShell = CreateObject("WScript.Shell")
WshShell.CurrentDirectory = CreateObject("Scripting.FileSystemObject").GetParentFolderName(WScript.ScriptFullName)
WshShell.Run "python main.py", 0, False
WScript.Sleep 2000
WshShell.Run "http://localhost:8080", 1, False
