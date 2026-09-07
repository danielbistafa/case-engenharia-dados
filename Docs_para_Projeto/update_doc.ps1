$word = New-Object -ComObject Word.Application
$word.Visible = $false
$doc = $word.Documents.Open('C:\Santander\DATA_Master\Docs_para_Projeto\Case Engenharia de dados.docx')

$texto = "Este desafio tem como objetivo desenvolver uma solução de Engenharia de Dados, que deverá abranger os seguintes tópicos: extração de dados, ingestão de dados, armazenamento de dados, observabilidade, segurança de dados, mascaramento de dados, arquitetura de dados e escalabilidade."

$doc.Content.Text = $texto
$doc.Save()
$doc.Close()
$word.Quit()
[System.Runtime.Interopservices.Marshal]::ReleaseComObject($word) | Out-Null