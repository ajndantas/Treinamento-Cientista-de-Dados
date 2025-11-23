Private Sub Workbook_Open()

Reiniciar:

    usuario = InputBox("Digite o seu usuario: ")
    senha = InputBox("Digite a sua senha: ")

    On Error Resume Next

    senha_certa = Worksheets("Logins").Cells.Find(usuario).Offset(0, 1)

    If (senha & "" <> senha_certa & "") Or usuario = "" Then
        GoTo Reiniciar
    End If
        
End Sub 

Private Sub Workbook_SheetDeactivate(ByVal Sh As Object)
    Worksheets("Planilha1").Activate
End Sub
