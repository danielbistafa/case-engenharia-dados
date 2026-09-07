# Criação de Contas e Acesso às Plataformas

Este guia contém os passos para criar as contas necessárias ao case. **Eu (assistente) não posso criar contas em seu nome**, pois exigem dados pessoais, e-mail, telefone e, no caso da Azure, cartão de crédito.

---

## 1. GitHub (gratuito)

1. Acesse: https://github.com/signup
2. Preencha e-mail, senha e nome de usuário.
3. Confirme o e-mail.
4. Escolha o plano gratuito.
5. Após criar, anote:
   - **Nome de usuário**
   - **E-mail**

### Criar o repositório

1. No GitHub, clique em **New repository**.
2. Nome: `case-engenharia-dados` (sugestão).
3. Deixe **público**.
4. **Não** inicialize com README (já temos um local).
5. Copie a URL HTTPS, exemplo: `https://github.com/SEU_USUARIO/case-engenharia-dados.git`

### Vincular repositório local ao remoto

Após criar, volte aqui e execute no PowerShell:

```powershell
cd C:\SANTANDER\DATA_Master
git remote add origin https://github.com/SEU_USUARIO/case-engenharia-dados.git
git branch -M main
git push -u origin main
```

Quando solicitado, use seu **Personal Access Token (PAT)** ou autenticação GitHub (senha não funciona mais para HTTPS no GitHub; use o Git Credential Manager instalado com o Git Portable).

---

## 2. Databricks Community Edition (gratuito)

1. Acesse: https://community.cloud.databricks.com/
2. Clique em **Sign Up**.
3. Preencha nome, e-mail e empresa/instituição.
4. Confirme o e-mail.
5. Faça login.
6. No console, você terá acesso a:
   - Clusters single-node gratuitos
   - Notebooks PySpark/Scala/SQL
   - DBFS (Databricks File System)
   - Jobs limitados

### O que fazer após criar

- Criar um cluster pequeno (`Single Node`, `Runtime 15.x`, tipo `r3.xlarge` ou similar).
- Criar um notebook para testar `spark.range(10).show()`.

---

## 3. Microsoft Azure Free Trial

⚠️ **Atenção:** exige cartão de crédito, mas **não cobra nos primeiros 30 dias** enquanto estiver dentro do crédito de US$ 200.

1. Acesse: https://azure.microsoft.com/free/
2. Clique em **Start free**.
3. Faça login com uma conta Microsoft (pode criar uma se não tiver).
4. Preencha telefone e cartão de crédito (apenas para verificação).
5. Aguarde aprovação (geralmente imediata).

### Controle de custos

- Crie um **Resource Group** exclusivo para o case.
- Após a apresentação, **exclua o Resource Group** para parar todos os custos.
- Desligue clusters Databricks quando não estiver usando.
- Não use SQL Warehouse para dashboard; use notebooks ou HTML local.

### Recursos que criaremos no Azure

- Resource Group
- Storage Account (ADLS Gen2)
- Databricks Workspace (pay-as-you-go)
- Opcional: Azure Key Vault para secrets

---

## Próximos passos

Após criar as contas, compartilhe comigo:

1. Seu usuário do GitHub.
2. Se o Databricks Community está funcionando.
3. Se a Azure Free Trial foi aprovada.

Assim, seguiremos para a Fase 1 (arquitetura) e Fase 2 (aquisição dos dados).
