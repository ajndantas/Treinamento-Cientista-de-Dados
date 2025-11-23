Sub InsertDataIntoOracle()

  ' **1. Define Connection String**
  Dim connStr As String
  connStr = "Provider=OraOLEDB.Oracle;Data Source=(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(Host=your_host_name)(Port=1521))(CONNECT_DATA=(SERVICE_NAME=your_service_name)));User ID=your_username;Password=your_password;"

  ' **2. Create Connection Object**
  Dim conn As ADODB.Connection
  Set conn = New ADODB.Connection

  ' **3. Open Connection**
  On Error Resume Next
  conn.Open connStr
  If Err.Number <> 0 Then
    MsgBox "Error connecting to Oracle: " & Err.Description
    Exit Sub
  End If
  On Error GoTo 0

  ' **4. Define SQL Insert Statement**
  Dim sql As String
  sql = "INSERT INTO your_table_name (column1, column2, ...) VALUES (?, ?, ...)"

  ' **5. Create Command Object**
  Dim cmd As ADODB.Command
  Set cmd = New ADODB.Command
  cmd.CommandText = sql
  cmd.CommandType = adCmdText
  cmd.ActiveConnection = conn

  ' **6. Set Parameter Values (Replace with your actual data)**
  Dim param1 As Variant
  Dim param2 As Variant
  ' ...
  param1 = "value1"
  param2 = "value2"
  ' ...
  cmd.Parameters.Append cmd.CreateParameter(0, adVarChar, adParamInput, 50, param1)
  cmd.Parameters.Append cmd.CreateParameter(1, adVarChar, adParamInput, 50, param2)
  ' ...

  ' **7. Execute the Insert Statement**
  On Error Resume Next
  cmd.Execute
  If Err.Number <> 0 Then
    MsgBox "Error inserting data: " & Err.Description
  Else
    MsgBox "Data inserted successfully!"
  End If
  On Error GoTo 0

  ' **8. Close Connection**
  conn.Close
  Set cmd = Nothing
  Set conn = Nothing

End Sub