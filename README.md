# Cognitivo AI

Consiste em ser um projeto de ETL de um aquivo CSV para parquet. Além da escolha do arquivo para escrita como 'output', o dataframe foi construido com um schema customizavel, feito a partir de um arquivo json, e também foi feito uma deduplicação nas entradas do arquivo csv, mantendo os registros mais recentes de cada id.  A escolha do arquivo para parquet foi feito devido algumas vantagens, citadas abaixo.  

  - Sua representação colunar para qualquer ecosistema Hadoop, o qual o Spark faz parte. 
  - A sua estrutura de dados complexas e infinitamente encadeáveis. 
  - Ele suporta outros algoritmos de compressão além do zip, como gzip e o snappy. Apesar de não ser usado nesse projeto, ele é usado em projetos com uma volumetria de dados maior. 
  - Um fator muito importante é que o parquet economiza muitos dados retirando os valores nulos no momento de realizar a escrita. 
  - Os metadados sobre cada coluna facilita no momento de download de arquivos parquet. Como ele tem o metadado de cada coluna, se há um filtro no momento de extrair os arquivos, há uma otimização nos dados lidos. 

Uma observação muito importante é que nesse projeto toda a execução foi feita localmente devido a volumetria dos dados e ao fato de eu não ter acesso a nenhuma cloud (sem que eu pague por ela). No caso de mesmo executar localmente, eu poderia ter usado o Apache Zeppelin, mas no momento só estou com um computador com Windows, o que não tem compatibilidade com o Zeppelin. Por esses motivos, eu utilizei somente o pyspark, um cluster no databricks para testes do notebook code.ipynb e o spark-shell para testes do code.py e functions.py. 
