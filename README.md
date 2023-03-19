# Desafio Data Lakehouse utilizando o Delta Lake.
Neste projeto, criei meu próprio Data Lakehouse usando o Delta Lake.

### Arquivos de fonte de dados
Utilizei as tabelas "device" e "subscription".

### Regras de junção
Optei por usar JOIN utilizando a chave "user_id", uma vez que não encontrei documentação para utilizar o auto-increment no Delta Table OSS. Aparentemente, existe uma issue sobre esse problema.

### Regras de agregação
Realizei a agregação dos dados, utilizando contagem ou outras transformações analíticas, utilizando tanto os arquivos atuais quanto o repositório.

#### Tabela OBT
- Criei uma tabela a partir do join das tabelas "device" e "subscription", selecionando algumas colunas que julguei serem importantes. A ideia era criar uma tabela ampla que pudesse ser usada pelo time de DS.

#### Tabela partnerships
- Criei uma visão do número de compras feitas através de cada método de pagamento.

#### Tabela devices_count_by_users
- Criei uma visão da quantidade de dispositivos por usuário e sua classificação dentro de um ranking.

#### Tabela devices
- Criei uma tabela que contém informações sobre os dispositivos dos usuários, que serão usadas para análise posterior. Não realizei qualquer agregação, com o objetivo de entregar uma réplica do banco para que os analistas possam explorar os dados sem qualquer viés ou regra atrelada. Também adicionei um rastreador de metadados, que captura o tempo de ingestão da bronze, silver e gold, e também fiz a partição por ano, mês e dia. Creio que, ao longo do tempo, as queries possam se tornar mais eficientes, mas há muitas coisas para se aprender para melhorar esta versão.

### Criei um arquivo readme.md explicando toda a minha implementação.

- Utilizei a arquitetura Medallion (Bronze, Silver e Gold) para a entrega dos dados. A ideia geral é entregar os dados na camada bronze e silver apenas fazendo append, e, assim, gerar microvisões na camada gold com overwrite. A landing (append moved data) é seguida pela bronze (append), pela silver (append), pela gold (overwrite) e, por fim, pela landing (processed history). A ideia é deixar a área de landing apenas com os dados que foram incrementais, para que não seja necessário realizar full nas camadas bronze e silver.

### Observações
Criei um arquivo .env com as seguintes variáveis:
```
ENDPOINT = http://xxxx
ACCESS_KEY = xxxx
SECRET_KEY = xxxx
```
Utilizei o MinIO, uma vez que, no Windows, não consegui criar as tabelas gold com o comando DeltaTable.createIfNotExists. Não entendi o motivo e deixei anotado para me aprofundar.
```
Bibliotecas
delta-spark==2.0.0
pyspark==3.2.2
Apache version 3.1.2
```

##### Anotações e GAPS:

- [ ] Validação da arquitetura Medallion (Bronze, Silver e Gold);
- [ ] Refatoração da lógica, utilizando melhores práticas;
- [ ] TheplumbersFlix, para aprender sobre conceitos e melhores práticas de mercado;
- [ ] Atualização das versões do Spark e do Delta local e ajuste dos jars;
- [ ] Atualização da versão do MinIO no k8s;
- [ ] Criação de uma imagem Docker;
- [ ] Implantação do Spark Operator.