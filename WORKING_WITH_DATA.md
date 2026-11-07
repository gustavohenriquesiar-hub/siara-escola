# Working with data — pipeline com tratamento de erros

Exemplo didático, sem dependências externas e sem dados pessoais. O workflow do GitHub Actions lê `sample_orders.json`, executa `data_pipeline.py` e guarda os resultados como artifact da execução.

## Etapas

1. **Estrutura:** JSON com listas de produtos e pedidos.
2. **Merge:** combina pedidos e produtos pela chave `sku`.
3. **Filter:** mantém pedidos pagos com quantidade positiva e registra os demais em `filtered-out.json`.
4. **Split:** separa os pedidos incluídos em lotes de dois arquivos em `batches/`.
5. **Summarize:** calcula pedidos, unidades e receita total e por categoria.
6. **Arquivos:** grava JSONs intermediários, resumo JSON/Markdown e lotes.
7. **Error workflow:** outro workflow acompanha a conclusão do pipeline e publica um resumo no Actions quando há falha. O tratamento fatal cobre JSON malformado, arquivo ausente, campos inválidos e erros de leitura/escrita.

## Como executar localmente

```bash
python data_pipeline.py --input sample_orders.json --output build/data-pipeline
```

No GitHub, o pipeline roda em push relacionado a esses arquivos, manualmente pela aba **Actions** ou toda segunda-feira às 12:30 UTC. Os arquivos gerados ficam disponíveis como artifact por sete dias.

As credenciais não são necessárias para esse exemplo. Se o próximo passo chamar uma API externa, guarde a chave em **Settings → Secrets and variables → Actions** e leia-a como variável de ambiente; nunca a coloque nos arquivos do repositório.
